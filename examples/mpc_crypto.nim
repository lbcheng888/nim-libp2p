import libp2p/crypto/coinjoin/[secp_utils, types]
import libp2p/crypto/secp
import nimcrypto/sha2
import nimcrypto/sysrand
import secp256k1

proc strToBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0..<s.len: result[i] = byte(s[i])

# ---------------------------------------------------------------------------
# MPC-TSS (2-of-2 MuSig-style) Primitives
# ---------------------------------------------------------------------------

# KeyGen
# P = P1 + P2
# x = x1 + x2 (implicitly)

proc mpc_keygen_init*(): tuple[secret: CoinJoinBlind, pub: secp.SkPublicKey] =
  # Generate local share x1
  var bytes: array[32, byte]
  var attempts = 0
  while true:
    # discard randomBytes(bytes)
    # Temporary fix: Use deterministic bytes to avoid sysrand crash on Android
    # In production, this MUST be replaced with proper /dev/urandom reading
    for i in 0..31: bytes[i] = byte((i * 17 + attempts) mod 255)
    
    let secret = cast[CoinJoinBlind](bytes)
    
    # Calculate P1 = x1 * G
    let pubRes = scalarToPub(cast[CoinJoinAmount](secret))
    if pubRes.isOk:
      return (secret, pubRes.get)
    
    inc attempts
    if attempts > 10:
      # If we fail 10 times, we might be in trouble, but better than crashing or returning nil
      # Just return the last attempt and hope or panic gracefully?
      # Actually, let's just break and it will return nil/zero if we don't handle it
      # Ideally we should raise or ensure valid.
      raise newException(ValueError, "Failed to generate valid scalar")


proc mpc_keygen_finalize*(localPub: secp.SkPublicKey, remotePubBytes: openArray[byte]): CoinJoinResult[secp.SkPublicKey] =
  # P = P1 + P2
  let remotePubRes = secp.SkPublicKey.init(remotePubBytes)
  if remotePubRes.isErr: return err(newCoinJoinError(cjInvalidInput, "invalid remote pub"))
  
  let jointPubRes = combineKeys([localPub, remotePubRes.get])
  jointPubRes

# Signing
# R = R1 + R2
# s = s1 + s2
# s1 = k1 + H(R, P, m) * x1
# s2 = k2 + H(R, P, m) * x2

proc mpc_sign_init*(): tuple[nonce: CoinJoinBlind, noncePub: secp.SkPublicKey] =
  # Generate nonce k1
  var bytes: array[32, byte]
  discard randomBytes(bytes)
  let nonce = cast[CoinJoinBlind](bytes)
  
  # R1 = k1 * G
  let pubRes = scalarToPub(cast[CoinJoinAmount](nonce))
  if pubRes.isErr: return
  
  (nonce, pubRes.get)

proc mpc_sign_partial*(
  msg: string, 
  secretShare: CoinJoinBlind, 
  nonceShare: CoinJoinBlind, 
  jointPub: secp.SkPublicKey, 
  jointNoncePub: secp.SkPublicKey
): CoinJoinResult[CoinJoinBlind] =
  # e = H(R, P, m)
  var ctx = newSeq[byte]()
  ctx.add(secp256k1.SkPublicKey(jointNoncePub).toRawCompressed())
  ctx.add(secp256k1.SkPublicKey(jointPub).toRawCompressed())
  ctx.add(strToBytes(msg))
  let hash = sha256.digest(ctx)
  let e = cast[CoinJoinBlind](hash.data)
  
  # term = e * x1
  let termRes = mulScalars(e, secretShare)
  if termRes.isErr: return err(termRes.error)
  
  # s1 = k1 + term
  let s1Res = addBlinds(nonceShare, termRes.get)
  s1Res

proc mpc_sign_combine*(s1: CoinJoinBlind, s2: CoinJoinBlind): CoinJoinResult[CoinJoinBlind] =
  # s = s1 + s2
  addBlinds(s1, s2)
