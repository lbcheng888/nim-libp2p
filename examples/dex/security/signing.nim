## Signing and verification helpers for dex orders/matches.

import std/[base64, json, options, os, strformat, strutils]

import pkg/results
import chronos
import libp2p/crypto/crypto
import libp2p/peerid
import stew/byteutils

import ../types

const
  DefaultKeyFile* = "node_ed25519.key"

type
  SigningMode* = enum
    smDisabled, smEnabled

  SigningConfig* = object
    mode*: SigningMode
    keyPath*: string
    requireRemoteSignature*: bool
    allowUnsigned*: bool

  SigningContext* = ref object
    cfg*: SigningConfig
    privKey: Option[PrivateKey]
    pubKey: Option[PublicKey]
    peerId: Option[PeerId]

proc ensureDir(path: string) =
  let (dir, _) = splitPath(path)
  if dir.len > 0 and not dirExists(dir):
    createDir(dir)

proc serializeKey(key: PrivateKey): string =
  let bytes = key.getBytes().expect("private key bytes")
  base64.encode(bytes)

proc serializePublicKey(key: PublicKey): string =
  let bytes = key.getBytes().expect("public key bytes")
  base64.encode(bytes)

proc loadKey(path: string): Option[(PrivateKey, PublicKey)] =
  if not fileExists(path):
    return none((PrivateKey, PublicKey))
  try:
    let raw = readFile(path)
    let parsed = parseJson(raw)
    if parsed.kind != JObject or not parsed.hasKey("privateKey"):
      return none((PrivateKey, PublicKey))
    let privateRaw = base64.decode(parsed["privateKey"].getStr())
    let privKey = PrivateKey.init(privateRaw).expect("load private key")
    var pubKey: PublicKey
    if parsed.hasKey("publicKey"):
      let publicRaw = base64.decode(parsed["publicKey"].getStr())
      pubKey = PublicKey.init(publicRaw).expect("load public key")
    else:
      pubKey = privKey.getPublicKey().expect("derive public key")
    some((privKey, pubKey))
  except CatchableError:
    none((PrivateKey, PublicKey))

proc storeKey(path: string; privKey: PrivateKey; pubKey: PublicKey) =
  ensureDir(path)
  let data = %*{
    "scheme": $privKey.scheme,
    "privateKey": privKey.serializeKey(),
    "publicKey": pubKey.serializePublicKey(),
  }
  writeFile(path, $data)

proc newKeyPair(): (PrivateKey, PublicKey) =
  var rng = newRng()
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).expect("generate keypair")
  (pair.seckey, pair.pubkey)

proc initSigning*(cfg: SigningConfig): SigningContext =
  result = SigningContext(cfg: cfg)
  if cfg.mode == smDisabled:
    return
  var priv: PrivateKey
  var pub: PublicKey
  let loaded = loadKey(cfg.keyPath)
  if loaded.isSome():
    (priv, pub) = loaded.get()
  else:
    (priv, pub) = newKeyPair()
    storeKey(cfg.keyPath, priv, pub)
  result.privKey = some(priv)
  result.pubKey = some(pub)
  let pidRes = PeerId.init(pub)
  if pidRes.isOk():
    result.peerId = some(pidRes.get())

proc canonicalOrderPayload*(order: OrderMessage): string =
  # Canonical representation for signing:
  # id|traderPeer|baseAssetChain/Symbol|quoteAssetChain/Symbol|side|price|amount|ttlMs|timestamp
  # Note: Adjust fields as necessary to match strictly what needs to be signed.
  &"{order.id}|{order.traderPeer}|{order.baseAsset.toString()}|{order.quoteAsset.toString()}|{order.side}|{order.price}|{order.amount}|{order.ttlMs}|{order.timestamp}"

proc canonicalMatchPayload*(match: MatchMessage): string =
  # Canonical representation for signing:
  # orderId|matcherPeer|baseAssetChain/Symbol|quoteAssetChain/Symbol|price|amount|note
  &"{match.orderId}|{match.matcherPeer}|{match.baseAsset.toString()}|{match.quoteAsset.toString()}|{match.price}|{match.amount}|{match.note}"

proc signBytes(ctx: SigningContext; data: openArray[byte]): Option[string] =
  if ctx.isNil() or ctx.privKey.isNone():
    return none(string)
  let sigRes = ctx.privKey.get().sign(data)
  if sigRes.isErr():
    return none(string)
  some(base64.encode(sigRes.get().data))

proc attachPublicKey(ctx: SigningContext; target: var string) =
  if ctx.pubKey.isSome():
    target = ctx.pubKey.get().serializePublicKey()

proc signOrder*(ctx: SigningContext; order: var OrderMessage) =
  if ctx.isNil() or ctx.cfg.mode == smDisabled:
    return
  let payload = canonicalOrderPayload(order)
  let sigOpt = ctx.signBytes(payload.toBytes())
  if sigOpt.isSome():
    order.signature = sigOpt.get()
    order.signatureVersion = 1
    ctx.attachPublicKey(order.signerPubKey)

proc signMatch*(ctx: SigningContext; match: var MatchMessage) =
  if ctx.isNil() or ctx.cfg.mode == smDisabled:
    return
  let payload = canonicalMatchPayload(match)
  let sigOpt = ctx.signBytes(payload.toBytes())
  if sigOpt.isSome():
    match.signature = sigOpt.get()
    match.signatureVersion = 1
    ctx.attachPublicKey(match.signerPubKey)

proc decodePublicKey(encoded: string): Option[PublicKey] =
  if encoded.len == 0:
    return none(PublicKey)
  try:
    let raw = base64.decode(encoded)
    some(PublicKey.init(raw).expect("public key init"))
  except CatchableError:
    none(PublicKey)

proc verifySignature(
    pubKey: PublicKey, payload: string, signature: string
): bool =
  if signature.len == 0:
    return false
  try:
    let sigBytes = base64.decode(signature)
    let sig = Signature.init(sigBytes).expect("signature init")
    sig.verify(payload.toBytes(), pubKey)
  except CatchableError:
    false

proc peerMatches(orderPeer: string, pubKey: PublicKey): bool =
  let pidRes = PeerId.init(pubKey)
  pidRes.isOk() and $pidRes.get() == orderPeer

proc verifyOrder*(ctx: SigningContext; order: OrderMessage): bool =
  if order.signature.len == 0:
    return ctx.cfg.allowUnsigned
  let pubOpt = decodePublicKey(order.signerPubKey)
  if pubOpt.isNone():
    return false
  let pubKey = pubOpt.get()
  if not peerMatches(order.traderPeer, pubKey):
    return false
  verifySignature(pubKey, canonicalOrderPayload(order), order.signature)

proc verifyMatch*(ctx: SigningContext; match: MatchMessage): bool =
  if match.signature.len == 0:
    return ctx.cfg.allowUnsigned
  let pubOpt = decodePublicKey(match.signerPubKey)
  if pubOpt.isNone():
    return false
  let pubKey = pubOpt.get()
  if not peerMatches(match.matcherPeer, pubKey):
    return false
  verifySignature(pubKey, canonicalMatchPayload(match), match.signature)
