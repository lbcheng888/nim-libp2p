# Nim-Libp2p CoinJoin secp256k1 helpers

{.push raises: [].}

import std/locks
import secp256k1, secp256k1/abi
import nimcrypto/sha2
import ../hkdf
import results

import ../secp
import ./types

const
  pedersenLabel = "coinjoin-blind"

var
  pedersenBaseCached: secp.SkPublicKey
  pedersenBaseInit = false
  pedersenBaseLock: Lock

initLock(pedersenBaseLock)

proc appendUint64(info: var seq[byte], value: uint64) =
  for i in countdown(7, 0):
    info.add byte((value shr (i * 8)) and 0xff)

proc appendUint32(info: var seq[byte], value: uint32) =
  for i in countdown(3, 0):
    info.add byte((value shr (i * 8)) and 0xff)

proc appendString(info: var seq[byte], s: string) =
  for ch in s:
    info.add byte(ch)

let emptySalt: array[0, byte] = []

proc toCoinJoinError(err: cstring, kind = cjInvalidInput): CoinJoinError =
  newCoinJoinError(kind, $err)

proc serializePoint*(pk: secp.SkPublicKey): CoinJoinPoint =
  let raw = secp256k1.SkPublicKey(pk).toRawCompressed()
  CoinJoinPoint(raw)

proc deserializePoint*(point: CoinJoinPoint): CoinJoinResult[secp.SkPublicKey] =
  let res = secp.SkPublicKey.init(point)
  if res.isErr:
    err(newCoinJoinError(cjInvalidInput, $res.error))
  else:
    ok(res.get)

proc scalarToPub*(scalar: CoinJoinAmount): CoinJoinResult[secp.SkPublicKey] =
  let privRes = SkPrivateKey.init(scalar)
  if privRes.isErr:
    return err(toCoinJoinError(privRes.error))
  var priv = privRes.get
  let pub = priv.getPublicKey()
  priv.clear()
  ok(pub)

proc getPedersenBase(): CoinJoinResult[secp.SkPublicKey] =
  pedersenBaseLock.acquire()
  defer:
    pedersenBaseLock.release()
  if pedersenBaseInit:
    return ok(pedersenBaseCached)
  var counter: uint32 = 0
  while counter < 1_000'u32:
    var ctx = newSeq[byte]()
    ctx.appendString(pedersenLabel)
    ctx.appendUint32(counter)
    let digest = sha256.digest(ctx)
    let res = SkPrivateKey.init(digest.data)
    if res.isOk:
      var sk = res.get
      let pk = sk.getPublicKey()
      sk.clear()
      pedersenBaseCached = pk
      pedersenBaseInit = true
      return ok(pk)
    inc counter
  err(newCoinJoinError(cjInternal, "unable to derive pedersen base"))

proc publicKeyNegate(pk: secp.SkPublicKey): CoinJoinResult[secp.SkPublicKey] =
  var raw = secp256k1.SkPublicKey(pk)
  if secp256k1_ec_pubkey_negate(secp256k1_context_no_precomp, cast[ptr secp256k1_pubkey](addr raw)) != 1:
    err(newCoinJoinError(cjInternal, "pubkey negate failed"))
  else:
    ok(secp.SkPublicKey(raw))

proc publicKeyTweakMul(base: secp.SkPublicKey, scalar: CoinJoinBlind): CoinJoinResult[secp.SkPublicKey] =
  var raw = secp256k1.SkPublicKey(base)
  var tweak = scalar
  if secp256k1_ec_pubkey_tweak_mul(secp256k1_context_no_precomp, cast[ptr secp256k1_pubkey](addr raw), addr tweak[0]) != 1:
    err(newCoinJoinError(cjInvalidInput, "invalid tweak for pubkey"))
  else:
    ok(secp.SkPublicKey(raw))

proc publicKeyCombine(keys: openArray[secp.SkPublicKey]): CoinJoinResult[secp.SkPublicKey] =
  if keys.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "no pubkeys to combine"))
  var rawKeys = newSeq[secp256k1.SkPublicKey](keys.len)
  var ptrs = newSeq[ptr secp256k1_pubkey](keys.len)
  for i, key in keys:
    rawKeys[i] = secp256k1.SkPublicKey(key)
    ptrs[i] = cast[ptr secp256k1_pubkey](addr rawKeys[i])
  var outKey {.noinit.}: secp256k1_pubkey
  if secp256k1_ec_pubkey_combine(
      secp256k1_context_no_precomp,
      addr outKey,
      cast[ptr ptr secp256k1_pubkey](ptrs[0].unsafeAddr),
      csize_t(keys.len)
    ) != 1:
    err(newCoinJoinError(cjInternal, "pubkey combine failed"))
  else:
    ok(secp.SkPublicKey(cast[secp256k1.SkPublicKey](outKey)))

proc scalarAdd(a, b: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] =
  var acc = a
  var tweak = b
  if secp256k1_ec_privkey_tweak_add(secp256k1_context_no_precomp, addr acc[0], addr tweak[0]) != 1:
    err(newCoinJoinError(cjInvalidInput, "blind addition overflows"))
  else:
    ok(acc)

proc scalarMul*(a, b: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] =
  var acc = a
  var tweak = b
  if secp256k1_ec_privkey_tweak_mul(secp256k1_context_no_precomp, addr acc[0], addr tweak[0]) != 1:
    err(newCoinJoinError(cjInvalidInput, "blind multiplication failed"))
  else:
    ok(acc)

proc scalarNegate(value: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] =
  var data = value
  if secp256k1_ec_privkey_negate(secp256k1_context_no_precomp, addr data[0]) != 1:
    err(newCoinJoinError(cjInternal, "blind negate failed"))
  else:
    ok(data)

proc scalarSub(a, b: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] =
  let neg = ?scalarNegate(b)
  scalarAdd(a, neg)

proc hkdfBlind*(masterSeed: openArray[byte], sessionId, nonce: uint64): CoinJoinResult[CoinJoinBlind] =
  if masterSeed.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "master seed required"))
  var info = newSeq[byte]()
  info.appendString(pedersenLabel)
  info.appendUint64(sessionId)
  info.appendUint64(nonce)
  var outBlind: array[1, CoinJoinBlind]
  sha256.hkdf(emptySalt, masterSeed, info, outBlind)
  var zero = true
  for b in outBlind[0]:
    if b != 0:
      zero = false
      break
  if zero:
    err(newCoinJoinError(cjInternal, "hkdf produced zero blind"))
  else:
    ok(outBlind[0])

proc encodeAmount*(value: uint64): CoinJoinAmount =
  var data: CoinJoinAmount
  for i in 0 ..< 8:
    data[CoinJoinAmount.len - 1 - i] = byte((value shr (i * 8)) and 0xff)
  data

proc amountScalar*(amount: CoinJoinAmount): CoinJoinAmount = amount

proc commitmentPoint*(pub: secp.SkPublicKey): CoinJoinPoint = serializePoint(pub)

proc commitmentPub*(commitment: CoinJoinCommitment): CoinJoinResult[secp.SkPublicKey] =
  deserializePoint(commitment.point)

proc pedersenBase*(): CoinJoinResult[secp.SkPublicKey] = getPedersenBase()

proc pedersenGeneratorMul*(blind: CoinJoinBlind): CoinJoinResult[secp.SkPublicKey] =
  let base = ?getPedersenBase()
  publicKeyTweakMul(base, blind)

proc generatorMul*(amount: CoinJoinAmount): CoinJoinResult[secp.SkPublicKey] =
  scalarToPub(amount)

proc addBlinds*(a, b: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] = scalarAdd(a, b)

proc subBlinds*(a, b: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] = scalarSub(a, b)

proc combineKeys*(keys: openArray[secp.SkPublicKey]): CoinJoinResult[secp.SkPublicKey] =
  publicKeyCombine(keys)

proc mulScalars*(a, b: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] = scalarMul(a, b)

proc negateKey*(key: secp.SkPublicKey): CoinJoinResult[secp.SkPublicKey] =
  publicKeyNegate(key)
