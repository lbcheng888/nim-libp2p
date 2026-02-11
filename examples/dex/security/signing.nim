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

proc peerIdStr*(ctx: SigningContext): string =
  if ctx.isNil() or ctx.peerId.isNone():
    return ""
  $ctx.peerId.get()

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
    let privateRaw = base64.decode(parsed["privateKey"].getStr()).toBytes()
    let privRes = PrivateKey.init(privateRaw)
    if privRes.isErr():
      return none((PrivateKey, PublicKey))
    let privKey = privRes.get()
    var pubKey: PublicKey
    if parsed.hasKey("publicKey"):
      let publicRaw = base64.decode(parsed["publicKey"].getStr()).toBytes()
      let pubRes = PublicKey.init(publicRaw)
      if pubRes.isErr():
        return none((PrivateKey, PublicKey))
      pubKey = pubRes.get()
    else:
      let pubRes = privKey.getPublicKey()
      if pubRes.isErr():
        return none((PrivateKey, PublicKey))
      pubKey = pubRes.get()
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
  let price = formatFloat(order.price, ffDecimal, 8)
  let amount = formatFloat(order.amount, ffDecimal, 8)
  &"{order.id}|{order.traderPeer}|{order.baseAsset.toString()}|{order.quoteAsset.toString()}|{order.side}|{price}|{amount}|{order.ttlMs}|{order.timestamp}"

proc canonicalMatchPayload*(match: MatchMessage): string =
  # Canonical representation for signing:
  # orderId|matcherPeer|baseAssetChain/Symbol|quoteAssetChain/Symbol|price|amount|note
  let price = formatFloat(match.price, ffDecimal, 8)
  let amount = formatFloat(match.amount, ffDecimal, 8)
  &"{match.orderId}|{match.matcherPeer}|{match.baseAsset.toString()}|{match.quoteAsset.toString()}|{price}|{amount}|{match.note}"

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
    let raw = base64.decode(encoded).toBytes()
    let pubRes = PublicKey.init(raw)
    if pubRes.isErr():
      return none(PublicKey)
    some(pubRes.get())
  except CatchableError:
    none(PublicKey)

proc verifySignature(
    pubKey: PublicKey, payload: string, signature: string
): bool =
  if signature.len == 0:
    return false
  try:
    let sigBytes = base64.decode(signature).toBytes()
    let sigRes = Signature.init(sigBytes)
    if sigRes.isErr():
      return false
    sigRes.get().verify(payload.toBytes(), pubKey)
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
