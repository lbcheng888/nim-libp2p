# Nim-LibP2P

import unittest2
import std/[times, bitops, options]
import pkg/results

import ../libp2p/[record, multiaddress, peerid, crypto/crypto, routing_record, signed_envelope]
import ../libp2p/protobuf/minprotobuf

proc strToBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, c in s:
    result[i] = byte(c)

proc buildIpnsEntry(
    priv: PrivateKey,
    peerId: PeerId,
    value: string,
    validitySeconds: int,
    includePubKey = false,
    validityType: string = "EOL",
    ttlSeconds: int = 0,
): seq[byte] =
  let valueBytes = strToBytes(value)
  var validityBytes: seq[byte]
  var sigType = validityType
  if validityType.toUpperAscii() == "TTL":
    let ns = if ttlSeconds > 0: int64(ttlSeconds) * 1_000_000_000 else: int64(validitySeconds) * 1_000_000_000
    validityBytes = strToBytes($ns)
    sigType = "TTL"
  else:
    let expiry = now().inZone(utc()) + validitySeconds.seconds
    let validityStr = expiry.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
    validityBytes = strToBytes(validityStr)
    sigType = "EOL"
  let sigData = valueBytes & validityBytes & strToBytes(sigType)
  let sig = priv.sign(sigData).expect("ipns signature")
  var pb = initProtoBuffer()
  pb.write(1, valueBytes)
  pb.write(2, sig.data)
  pb.write(4, sigType)
  pb.write(5, validityBytes)
  pb.write(6, $1)
  if includePubKey:
    let pubBytes = priv.getPublicKey().expect("pub").getBytes().expect("pub bytes")
    pb.write(8, pubBytes)
  pb.finish()
  pb.buffer

suite "libp2p record":
  test "sign and verify record":
    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let rec = prepareRecord("/ipfs/example", @[byte 1, 2, 3], priv).expect("record")

    check rec.author == peerId
    check rec.verifySignature()

    let encoded = rec.encode()
    let decoded = Record.decode(encoded).expect("decode")
    check decoded.key == rec.key
    check decoded.value == rec.value
    check decoded.author == rec.author
    check decoded.verifySignature()

  test "record validator":
    var store = RecordStore.init()
    store.registerValidator("ipns", proc(key: string, value: seq[byte]): bool =
      key.startsWith("ipns/") and value.len > 0
    )

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    var rec = prepareRecord("ipns/example", @[byte 10], priv).expect("rec")
    check store.validate(rec)

    rec.value = @[]
    check store.validate(rec) == false

  test "record validator handles leading slash":
    var store = RecordStore.init()
    store.registerValidator("ipns", proc(key: string, value: seq[byte]): bool =
      key.startsWith("/ipns/") and value.len > 0
    )

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    var rec = prepareRecord("/ipns/example", @[byte 42], priv).expect("rec")
    check store.validate(rec)

    rec.value = @[]
    check store.validate(rec) == false

  test "record validator picks longest matching prefix":
    var store = RecordStore.init()
    store.registerValidator("ipns", proc(key: string, value: seq[byte]): bool =
      false
    )
    store.registerValidator("ipns/example", proc(key: string, value: seq[byte]): bool =
      value == @[byte 1]
    )

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let rec = prepareRecord("/ipns/example/data", @[byte 1], priv).expect("rec")
    check store.validate(rec)

  test "record validator supports default namespace":
    var store = RecordStore.init()
    store.registerValidator("", proc(key: string, value: seq[byte]): bool =
      value.len > 0
    )

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let rec = prepareRecord("custom", @[byte 2], priv).expect("rec")
    check store.validate(rec)

    rec.value = @[]
    check store.validate(rec) == false

  test "record validator trims trailing slash":
    var store = RecordStore.init()
    store.registerValidator("ipns/example", proc(key: string, value: seq[byte]): bool =
      value == @[byte 3]
    )

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let rec = prepareRecord("/ipns/example/", @[byte 3], priv).expect("rec")
    check store.validate(rec)

  test "peer record validator":
    var store = RecordStore.init()
    store.registerPeerRecordValidator()

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let addr = MultiAddress.init("/ip4/127.0.0.1/tcp/9000").tryGet()
    let record = PeerRecord.init(peerId, @[addr])
    let signed = SignedPayload[PeerRecord].init(priv, record).expect("signed")
    let encoded = signed.encode().expect("encode")

    let validRecord = Record(
      key: "/libp2p/peer-record/" & $peerId,
      value: encoded,
    )
    check store.validate(validRecord)

    let invalidKeyRecord = Record(
      key: "/libp2p/peer-record/12D3Wrong",
      value: encoded,
    )
    check store.validate(invalidKeyRecord) == false

  test "unregister validator removes mapping":
    var store = RecordStore.init()
    store.registerValidator("ipns", proc(key: string, value: seq[byte]): bool = false)
    store.unregisterValidator("ipns")

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let rec = prepareRecord("/ipns/example", @[byte 1], priv).expect("rec")
    check store.validate(rec) # no validator, defaults to true

  test "ipns validator for peer-based key":
    var store = RecordStore.init()
    store.registerIpnsValidator()

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let entry = buildIpnsEntry(priv, peerId, "payload", 3600, includePubKey = false)

    let record = Record(
      key: "/ipns/" & $peerId,
      value: entry,
    )
    check store.validate(record)

  test "ipns validator rejects invalid signature":
    var store = RecordStore.init()
    store.registerIpnsValidator()

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    var entry = buildIpnsEntry(priv, peerId, "payload", 3600)
    if entry.len > 10:
      entry[10] = entry[10] xor byte(0xFF)

    let record = Record(
      key: "/ipns/" & $peerId,
      value: entry,
    )
    check store.validate(record) == false

  test "ipns validator allows non-peer names with embedded pubkey":
    var store = RecordStore.init()
    store.registerIpnsValidator()

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let entry = buildIpnsEntry(priv, peerId, "payload", 3600, includePubKey = true)

    let record = Record(
      key: "/ipns/example.com",
      value: entry,
    )
    check store.validate(record)

  test "ipns validator rejects unregistered namespace":
    var store = RecordStore.init()
    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let entry = buildIpnsEntry(priv, peerId, "payload", 7200, includePubKey = true)

    let record = Record(
      key: "/ipns/custom/ns/example",
      value: entry,
    )
    check store.validate(record) == false

  test "ipns namespace policy enforces ttl and namespace":
    var store = RecordStore.init()
    store.registerIpnsNamespace("custom", IpnsNamespacePolicy(
      requirePeerId: false,
      requirePublicKey: true,
      maxRecordLifetime: some(1.hours),
      maxCacheDuration: some(20.minutes),
      refreshWeight: DefaultIpnsRefreshWeight,
    ))

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let entry = buildIpnsEntry(priv, peerId, "payload", 7200, includePubKey = true)

    let record = Record(
      key: "/ipns/custom/example.com",
      value: entry,
    )
    check store.validate(record)

    let cachedOpt = store.getIpnsRecord("/ipns/custom/example.com")
    check cachedOpt.isSome()
    let cached = cachedOpt.get()
    check cached.namespace == "custom"

    let nowUtc = now().inZone(utc())
    let maxLifetime = initDuration(hours = 1) + 5.seconds
    let maxCache = initDuration(minutes = 20) + 5.seconds
    let lifetime = cached.validUntil - nowUtc
    let cacheLifetime = cached.cacheUntil - nowUtc
    check lifetime <= maxLifetime
    check cacheLifetime <= maxCache

  test "ipns validator accepts ttl type":
    var store = RecordStore.init()
    store.registerIpnsValidator()

    let rng = newRng()
    let priv = PrivateKey.random(rng[]).expect("priv")
    let peerId = PeerId.init(priv).expect("peer")
    let entry = buildIpnsEntry(
      priv, peerId, "payload", 3600, includePubKey = false, validityType = "TTL", ttlSeconds = 5
    )

    let record = Record(
      key: "/ipns/" & $peerId,
      value: entry,
    )
    check store.validate(record)
