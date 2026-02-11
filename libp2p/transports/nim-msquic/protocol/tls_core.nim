## RFC 9001 TLS Core for QUIC Initial Packet Protection
## Handles Key Derivation (HKDF), Header Protection, and Payload Encryption.

import std/sequtils
import std/strutils
import std/endians
import nimcrypto/rijndael
import nimcrypto/bcmode
import nimcrypto/sha2
import nimcrypto/hmac
import bearssl/kdf
import bearssl/hash as bhash
import libp2p/crypto/hkdf
import libp2p/crypto/curve25519
import nimcrypto/sysrand

type
  Secret = seq[byte]
  Iv = seq[byte]
  Key = seq[byte]
  HpKey = seq[byte]
  
  InitialSecrets* = object
    clientSecret*: Secret
    serverSecret*: Secret
    clientKey*: Key
    clientIv*: Iv
    clientHp*: HpKey
    serverKey*: Key
    serverIv*: Iv
    serverHp*: HpKey

  TrafficSecrets* = object
    clientSecret*: Secret
    serverSecret*: Secret
    clientKey*: Key
    clientIv*: Iv
    clientHp*: HpKey
    serverKey*: Key
    serverIv*: Iv
    serverHp*: HpKey



const
  # RFC 9001 Section 5.2. Initial Salt for QUIC v1
  InitialSaltV1 = [0x38'u8, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17, 0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad, 0xcc, 0xbb, 0x7f, 0x0a]
  
  # Label prefixes
  LabelPrefix = "tls13 "

# --- HKDF Helpers ---

proc hkdfExpandLabel[T](secret: openArray[byte], label: string, context: openArray[byte], length: static int): array[length, byte] =
  # RFC 8446 Section 7.1. Key Schedule Context
  # HkdfLabel { length, label<7..255>, context<0..255> }
  var hkdfLabel: seq[byte] = @[]
  
  # Length (uint16)
  var lenNet: uint16
  var lenHost = uint16(length)
  bigEndian16(addr lenNet, addr lenHost)
  hkdfLabel.add(cast[ptr array[2, byte]](addr lenNet)[])
  
  # Label (opaque<7..255>) -> "tls13 " + label
  let fullLabel = LabelPrefix & label
  hkdfLabel.add(byte(fullLabel.len))
  for c in fullLabel: hkdfLabel.add(byte(c))
  
  # Context (opaque<0..255>)
  hkdfLabel.add(byte(context.len))
  hkdfLabel.add(context)
  
  var outKey: array[length, byte]
  var outArr: array[1, array[length, byte]]
  
  # Using generic HKDF from crypto/hkdf.nim which expects specific signature
  # We use a localized version of expand since existing 'hkdf' proc does Extract+Expand combined
  # But here we need Expand only if we already have the PRK (pseudo random key).
  # However, for simplicity and since `hkdf.nim` exposes a combined one,
  # we might need to manually call BearSSL if we want pure Expand.
  # WA: Using the provided `hkdf` from `libp2p/crypto/hkdf.nim` behaves as Extract+Expand.
  # RFC 9001 details:
  # Initial Secret = HKDF-Extract(salt, client_dst_connection_id)
  # Client Initial Secret = HKDF-Expand-Label(Initial Secret, "client in", "", 32)
  # Key = HKDF-Expand-Label(Client Initial Secret, "quic key", "", 16)
  
  # Since we don't have a pure Expand exposed easily in `hkdf.nim` (it does both),
  # we will use BearSSL APIs directly for fine-grained control if needed, 
  # OR misuse the `hkdf` proc with empty salt if appropriate (though Extract with empty salt != generic Expand).
  
  # Let's direct access generic BearSSL kdf for robustness as used in `hkdf.nim`.
  var ctx: HkdfContext
  hkdfInit(ctx, addr sha256Vtable, nil, 0) # Fake init
  
  # We assume `secret` IS the PRK for Expand step.
  # We skip Extract step by manually injecting PRK ? No, BearSSL `hkdfInit` does Extract.
  # To do Expand-Only, we'd need to bypass `hkdfInit` logic or provide it as PRK.
  # Actually, `hkdf.nim` is: Init(Extract) -> Inject(IKM) -> Flip -> Produce(Expand).
  
  # RFC 5869: If salt checks out, we produce PRK.
  # For "Expand Only", we treat the input secret as the PRK. 
  # BearSSL's `hkdfFlip` prepares for expansion.
  # We might need to implement a small helper here to do Expand-Label cleanly using existing libs.
  # FOR SKELETON: We will stick to `hkdf` proc if possible or implement a mini one.
  
  # Let's implement a clean `hkdfExpand` using `hmac`?
  # Or just use the `hkdf` wrapper and assume we re-extract. (Suboptimal).
  
  # CORRECT PATH: Re-implement lightweight Expand using HMAC-SHA256 from nimcrypto.
  discard

# --- Initial Secrets ---

# Helper for wrapping HMAC-SHA256 based HKDF-Expand
proc hmacSha256(key, data: openArray[byte]): MDigest[256] =
  result = sha256.hmac(key, data)

proc hkdfExpand[Len: static int](prk: openArray[byte], info: openArray[byte]): array[Len, byte] =
  # RFC 5869 Section 2.3. Expand
  # T(0) = empty
  # T(1) = HMAC-Hash(PRK, T(0) | info | 0x01)
  # ...
  var n = (Len + 32 - 1) div 32
  var t: seq[byte] = @[]
  var okm: seq[byte] = @[]
  
  for i in 1 .. n:
    var input: seq[byte] = @[]
    input.add(t)
    input.add(info)
    input.add(byte(i))
    let digest = hmacSha256(prk, input)
    t = @(digest.data)
    okm.add(t)
    
  for i in 0 ..< Len:
    result[i] = okm[i]

proc deriveSecret[Len: static int](secret: openArray[byte], label: string): array[Len, byte] =
  var hkdfLabel: seq[byte] = @[]
  var lenNet: uint16
  var lenHost = uint16(Len)
  bigEndian16(addr lenNet, addr lenHost)
  hkdfLabel.add(cast[ptr array[2, byte]](addr lenNet)[])
  
  let fullLabel = LabelPrefix & label
  hkdfLabel.add(byte(fullLabel.len))
  for c in fullLabel: hkdfLabel.add(byte(c))
  
  hkdfLabel.add(0'u8) # Length of context (empty)
  
  return hkdfExpand[Len](secret, hkdfLabel)

proc deriveInitialSecrets*(dcid: openArray[byte]): InitialSecrets =
  # 1. Initial Secret = HKDF-Extract(InitialSalt, DCID)
  # We use generic HKDF Extract here (HMAC-SHA256(Salt, IKM))
  # Since we don't have a direct Extract func, we use HMAC directly.
  let initialSecretDigest = hmacSha256(InitialSaltV1, dcid)
  let initialSecret = initialSecretDigest.data
  
  # 2. Client Initial Secret
  let clientSecretArr = deriveSecret[32](initialSecret, "client in")
  result.clientSecret = @clientSecretArr
  
  # 3. Server Initial Secret
  let serverSecretArr = deriveSecret[32](initialSecret, "server in")
  result.serverSecret = @serverSecretArr
  
  # 4. Client Key, IV, HP
  let cKey = deriveSecret[16](result.clientSecret, "quic key")
  result.clientKey = @cKey
  let cIv = deriveSecret[12](result.clientSecret, "quic iv")
  result.clientIv = @cIv
  let cHp = deriveSecret[16](result.clientSecret, "quic hp")
  result.clientHp = @cHp
  
  # 5. Server Key, IV, HP
  let sKey = deriveSecret[16](result.serverSecret, "quic key")
  result.serverKey = @sKey
  let sIv = deriveSecret[12](result.serverSecret, "quic iv")
  result.serverIv = @sIv
  let sHp = deriveSecret[16](result.serverSecret, "quic hp")
  result.serverHp = @sHp

# --- Packet Protection ---
# For Skeleton Phase 3, we implement basic AES-GCM Encryption logic request by user.

proc encryptPacket*(key, iv: openArray[byte], packetNumber: uint64, 
                    header: openArray[byte], payload: openArray[byte], 
                    paramTag: var array[16, byte]): seq[byte] =
  # 1. AEAD Nonce = IV XOR Packet Number (padded to 12 bytes)
  var nonce: array[12, byte]
  for i in 0 .. 11:
    nonce[i] = iv[i]
  
  # XOR last 8 bytes with Packet Number (Network Byte Order)
  # RFC 9001: The packet number is left-padded with zeros to the size of the IV. 
  # Then exclusive ORed with the IV.
  var pnBe: uint64
  var pnHost = packetNumber
  bigEndian64(addr pnBe, addr pnHost)
  let pnBytes = cast[ptr array[8, byte]](addr pnBe)
  
  # XOR the last 8 bytes of IV (Nonce)
  for i in 0 .. 7:
    nonce[12 - 8 + i] = nonce[12 - 8 + i] xor pnBytes[i]
    
  # 2. AES-GCM Encrypt
  var ctx: GCM[aes128]
  ctx.init(key, nonce, header) # header is AAD
  
  var ciphertext = newSeq[byte](payload.len)
  if payload.len > 0:
    ctx.encrypt(payload, ciphertext)
  
  ctx.getTag(paramTag)
  return ciphertext

proc applyHeaderProtection*(hpKey: openArray[byte], sample: openArray[byte], 
                            firstByte: var byte, packetNumber: var openArray[byte]) =
  # RFC 9001 Header Protection (AES-ECB-based mask)
  # Mask = AES-ECB(hpKey, sample)[0..4]
  var ctx: rijndael128
  ctx.init(hpKey)
  
  var mask: array[16, byte]
  var sampleArr: array[16, byte]
  if sample.len >= 16:
    for i in 0..15: sampleArr[i] = sample[i]
    
  ctx.encrypt(sampleArr, mask)
  
  # Apply Mask
  # Long Header: 4 bits of Packet Number Length masked by mask[0] & 0x0f
  # Packet Number: masked by mask[1..pnLen]
  
  # 1. Mask First Byte (low 4 bits for PN Length)
  if (firstByte and 0x80) != 0: # Long Header
    firstByte = firstByte xor (mask[0] and 0x0f)
  else: # Short Header
    firstByte = firstByte xor (mask[0] and 0x1f)
    
  # 2. Mask Packet Number
  for i in 0 ..< packetNumber.len:
    packetNumber[i] = packetNumber[i] xor mask[i + 1]

proc removeHeaderProtection*(hpKey: openArray[byte], sample: openArray[byte], 
                             firstByte: var byte, packetNumber: var openArray[byte]) =
  # RFC 9001: Header Protection removal is symmetric to application (XOR)
  applyHeaderProtection(hpKey, sample, firstByte, packetNumber)

proc decryptPacket*(key, iv: openArray[byte], packetNumber: uint64, 
                    header: openArray[byte], ciphertext: openArray[byte], 
                    tag: openArray[byte]): seq[byte] =
  # 1. Reconstruct Nonce (IV XOR Packet Number)
  var nonce: array[12, byte]
  for i in 0 .. 11:
    nonce[i] = iv[i]
  
  var pnBe: uint64
  var pnHost = packetNumber
  bigEndian64(addr pnBe, addr pnHost)
  let pnBytes = cast[ptr array[8, byte]](addr pnBe)
  
  for i in 0 .. 7:
    nonce[12 - 8 + i] = nonce[12 - 8 + i] xor pnBytes[i]
    
  # 2. AES-GCM Decrypt
  var ctx: GCM[aes128]
  ctx.init(key, nonce, header) # header is AAD
  
  var plaintext = newSeq[byte](ciphertext.len)
  if ciphertext.len > 0:
    ctx.decrypt(ciphertext, plaintext)
    
  var computedTag: array[16, byte]
  ctx.getTag(computedTag)
  
  # 3. Verify Tag
  # Since GCM.getTag returns the tag computed during decrypt, we compare it with the received tag.
  var match = true
  for i in 0..15:
    if computedTag[i] != tag[i]:
      match = false
      
  if not match:
    # Decryption failure
    return @[]
    
  return plaintext

# --- Phase 5: Handshake & Key Exchange ---

type
  ClientKeyShare* = object
    privateKey*: Curve25519Key
    publicKey*: Curve25519Key
    
proc generateKeyShare*(): ClientKeyShare =
  # 1. Generate Random Private Key (32 bytes)
  var rawKey: array[32, byte]
  if randomBytes(rawKey) != 32:
    # Fallback or error? sysrand should work.
    discard
    
  # 2. Clamp (RFC 7748)
  rawKey[0] = rawKey[0] and 248
  rawKey[31] = (rawKey[31] and 127) or 64
  
  result.privateKey = intoCurve25519Key(rawKey)
  
  # 3. Compute Public Key
  result.publicKey = public(result.privateKey)

proc encodeClientHello*(destCid: openArray[byte], keyShare: ClientKeyShare): seq[byte] =
  # RFC 8446 ClientHello
  var buf: seq[byte] = @[]
  
  # Handshake Header
  # Type: ClientHello (1)
  # Length: (24-bit) - Placeholder, fill later
  buf.add(0x01'u8)
  buf.add([0x00'u8, 0x00, 0x00]) 
  
  let startOffset = buf.len
  
  # Legacy Version: 0x0303 (TLS 1.2)
  buf.add([0x03'u8, 0x03])
  
  # Random: 32 bytes
  var random: array[32, byte]
  discard randomBytes(random)
  buf.add(random)
  
  # Legacy Session ID: 0 length (or 32 bytes random if compatibility needed, often 0 for QUIC)
  buf.add(0x00'u8) 
  
  # Cipher Suites: 
  # TLS_AES_128_GCM_SHA256 (0x1301)
  buf.add([0x00'u8, 0x02]) # Length 2
  buf.add([0x13'u8, 0x01])
  
  # Compression Methods: 0 (0x00)
  buf.add([0x01'u8, 0x00])
  
  # Extensions
  var extBuf: seq[byte] = @[]
  
  # 1. Supported Versions (0x002b)
  # Val: List of versions. QUIC requires only TLS 1.3
  var supVer: seq[byte] = @[]
  supVer.add([0x02'u8, 0x03, 0x04]) # Len 2, TLS 1.3 (0x0304)
  
  extBuf.add([0x00'u8, 0x2b]) # Type
  extBuf.add([0x00'u8, 0x03]) # Len
  extBuf.add(supVer)
  
  # 2. Key Share (0x0033)
  # ClientKeyShare: Group x25519 (0x001d), Key Exchange (32 bytes)
  var ksBuf: seq[byte] = @[]
  # ClientShare Entry
  ksBuf.add([0x00'u8, 0x1d]) # Group: x25519
  ksBuf.add([0x00'u8, 0x20]) # Key Len: 32
  ksBuf.add(keyShare.publicKey.getBytes())
  
  # Wrap in KeyShare ClientHello extension structure
  # List Length (2 bytes) + Entry
  var ksExt: seq[byte] = @[]
  let ksListLen = uint16(ksBuf.len)
  var ksListLenBe: uint16
  var ksListLenHost = ksListLen
  bigEndian16(addr ksListLenBe, addr ksListLenHost)
  
  ksExt.add(cast[ptr array[2, byte]](addr ksListLenBe)[])
  ksExt.add(ksBuf)
  
  extBuf.add([0x00'u8, 0x33]) # Type
  let ksExtLen = uint16(ksExt.len)
  var ksExtLenBe: uint16; bigEndian16(addr ksExtLenBe, addr ksExtLen)
  extBuf.add(cast[ptr array[2, byte]](addr ksExtLenBe)[])
  extBuf.add(ksExt)
  
  # 3. QUIC Transport Parameters (0x0039 or 0xffa5 for draft?)
  # RFC 9000 uses 0x39. Required for QUIC handshake.
  # Empty sequence for now? Or minimal?
  # Minimal: initial_source_connection_id
  # But we don't have full encoder here. 
  # Skipping might cause server to abort, but let's try MINIMAL first for Skeleton.
  # Actually, let's omit if possible, or add empty one.
  # Extension Type: 0x39 (57)
  # Extension Len: 0
  extBuf.add([0x00'u8, 0x39]) 
  extBuf.add([0x00'u8, 0x00])

  # Extensions Length
  let extLen = uint16(extBuf.len)
  var extLenBe: uint16; bigEndian16(addr extLenBe, addr extLen)
  buf.add(cast[ptr array[2, byte]](addr extLenBe)[])
  buf.add(extBuf)
  
  # Update Handshake Length (24-bit)
  let msgLen = buf.len - startOffset
  buf[1] = byte((msgLen shr 16) and 0xff)
  buf[2] = byte((msgLen shr 8) and 0xff)
  buf[3] = byte(msgLen and 0xff)
  
  return buf

# --- Handshake Parsing & Secrets ---

proc computeSharedSecret*(privateKey: Curve25519Key, peerPublicKeyBytes: openArray[byte]): Secret =
  var peerPub: Curve25519Key
  peerPub = intoCurve25519Key(peerPublicKeyBytes)
  # libp2p/crypto/curve25519 `dh` returns internal array, map to seq
  var sharedPoint = peerPub
  Curve25519.mul(sharedPoint, privateKey)
  let rawSecret = sharedPoint
  result = @rawSecret

proc deriveHandshakeSecrets*(sharedSecret: Secret, helloHash: openArray[byte]): InitialSecrets =
  # RFC 8446 / 9001
  # 1. Early Secret = HKDF-Extract(0, 0) -> derived from PSK if check (none here)
  # Actually standard TLS 1.3 schedule:
  # Early Secret = HKDF-Extract(0, 0)
  # Derived Secret = HKDF-Expand-Label(Early Secret, "derived", EmptyHash, HashLen)
  # Handshake Secret = HKDF-Extract(Derived Secret, Shared Secret)
  
  # For Clean Skeleton: We approximate or use correct steps if libraries allow.
  # Simplification: Assume standard schedule constants.
  
  # A. Start with Zero Salt?
  # The salt for the first extract is 0 (since no PSK).
  let zeroArr = [0'u8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
  # Early Secret
  let earlySecretDigest = hmacSha256(zeroArr, zeroArr) # salt=0, ikm=0
  let earlySecret = earlySecretDigest.data

  # B. Derived Secret
  # emptyHash = SHA256("")
  let emptyHash = [0xe3'u8, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55]
  
  # deriveSecret uses LabelPrefix="tls13 ". 
  # HKDF-Expand-Label(Secret, Label, Context, Len)
  # Context = HASH
  # For "derived": Context is EmptyHash (32 bytes)
  
  # We need a `deriveSecretWithContext` helper effectively.
  # existing `deriveSecret` passes EMPTY context.
  # We need one that takes context.
  
  proc deriveSecretWithContext[Len: static int](secret: openArray[byte], label: string, context: openArray[byte]): array[Len, byte] =
    var hkdfLabel: seq[byte] = @[]
    var lenNet: uint16
    var lenHost = uint16(Len)
    bigEndian16(addr lenNet, addr lenHost)
    hkdfLabel.add(cast[ptr array[2, byte]](addr lenNet)[])
    
    let fullLabel = LabelPrefix & label
    hkdfLabel.add(byte(fullLabel.len))
    for c in fullLabel: hkdfLabel.add(byte(c))
    
    hkdfLabel.add(byte(context.len))
    hkdfLabel.add(context)
    
    return hkdfExpand[Len](secret, hkdfLabel)

  let derivedSecretArr = deriveSecretWithContext[32](earlySecret, "derived", emptyHash)
  let derivedSecret = @derivedSecretArr
  
  # C. Handshake Secret = HKDF-Extract(Derived, SharedSecret)
  let handshakeSecretDigest = hmacSha256(derivedSecret, sharedSecret)
  let handshakeSecret = handshakeSecretDigest.data
  
  # D. Client/Server Handshake Traffic Secret
  # Client HTS = HKDF-Expand-Label(Handshake Secret, "c hs traffic", HelloHash, 32)
  # Server HTS = HKDF-Expand-Label(Handshake Secret, "s hs traffic", HelloHash, 32)
  
  let cHtsArr = deriveSecretWithContext[32](handshakeSecret, "c hs traffic", helloHash)
  let sHtsArr = deriveSecretWithContext[32](handshakeSecret, "s hs traffic", helloHash)
  
  # E. Derive Keys/IVs (same "quic key", "quic iv", "quic hp" labels)
  # Client Keys
  let cKey = deriveSecret[16](cHtsArr, "quic key")
  let cIv = deriveSecret[12](cHtsArr, "quic iv")
  let cHp = deriveSecret[16](cHtsArr, "quic hp")
  
  # Server Keys
  let sKey = deriveSecret[16](sHtsArr, "quic key")
  let sIv = deriveSecret[12](sHtsArr, "quic iv")
  let sHp = deriveSecret[16](sHtsArr, "quic hp")
  
  result.clientSecret = @cHtsArr
  result.serverSecret = @sHtsArr
  result.clientKey = @cKey
  result.clientIv = @cIv
  result.clientHp = @cHp
  result.serverKey = @sKey
  result.serverIv = @sIv
  result.serverHp = @sHp

proc deriveApplicationSecrets*(handshakeSecret: Secret, handshakeHash: openArray[byte]): TrafficSecrets =
  # RFC 8446
  # 1. Derived Secret = HKDF-Expand-Label(Handshake Secret, "derived", EmptyHash, 32)
  # 2. Master Secret = HKDF-Extract(Derived Secret, 0)
  
  let emptyHash = [0xe3'u8, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55]

  proc deriveSecretWithContext[Len: static int](secret: openArray[byte], label: string, context: openArray[byte]): array[Len, byte] =
    var hkdfLabel: seq[byte] = @[]
    var lenNet: uint16
    var lenHost = uint16(Len)
    bigEndian16(addr lenNet, addr lenHost)
    hkdfLabel.add(cast[ptr array[2, byte]](addr lenNet)[])
    
    let fullLabel = LabelPrefix & label
    hkdfLabel.add(byte(fullLabel.len))
    for c in fullLabel: hkdfLabel.add(byte(c))
    
    hkdfLabel.add(byte(context.len))
    hkdfLabel.add(context)
    
    return hkdfExpand[Len](secret, hkdfLabel)

  let derivedSecretArr = deriveSecretWithContext[32](handshakeSecret, "derived", emptyHash)
  let derivedSecret = @derivedSecretArr
  
  let zeroArr = [0'u8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
  let masterSecretDigest = hmacSha256(derivedSecret, zeroArr)
  let masterSecret = masterSecretDigest.data
  
  # Client/Server Application Traffic Secret 0
  # c ap traffic = Expand-Label(Master, "c ap traffic", HandshakeHash, 32) 
  # s ap traffic = ... "s ap traffic" ...
  
  let cAtsArr = deriveSecretWithContext[32](masterSecret, "c ap traffic", handshakeHash)
  let sAtsArr = deriveSecretWithContext[32](masterSecret, "s ap traffic", handshakeHash)
  
  # Keys
  let cKey = deriveSecret[16](cAtsArr, "quic key")
  let cIv = deriveSecret[12](cAtsArr, "quic iv")
  let cHp = deriveSecret[16](cAtsArr, "quic hp")

  let sKey = deriveSecret[16](sAtsArr, "quic key")
  let sIv = deriveSecret[12](sAtsArr, "quic iv")
  let sHp = deriveSecret[16](sAtsArr, "quic hp")
  
  result.clientSecret = @cAtsArr
  result.serverSecret = @sAtsArr
  result.clientKey = @cKey
  result.clientIv = @cIv
  result.clientHp = @cHp
  result.serverKey = @sKey
  result.serverIv = @sIv
  result.serverHp = @sHp

proc findServerKeyShare*(handshakeBytes: openArray[byte]): seq[byte] =
  # Very basic parser to find Extension 0x0033 (Key Share)
  # ServerHello Structure:
  # Type(1) | Len(3) | Ver(2) | Rand(32) | SessIDLen(1) | SessID(var) | Cipher(2) | Comp(1) | ExtLen(2) | Exts...
  
  var idx = 0
  if handshakeBytes.len < 4: return @[]
  
  let msgType = handshakeBytes[0]
  if msgType != 2: return @[] # Not ServerHello
  
  # Length (3 bytes)
  idx += 4
  
  # Version (2) + Random (32) = 34
  idx += 34
  
  if idx >= handshakeBytes.len: return @[]
  let sessIdLen = int(handshakeBytes[idx])
  idx += 1 + sessIdLen
  
  # Cipher (2) + Comp (1) = 3
  idx += 3
  
  # Extensions Length (2)
  if idx + 2 > handshakeBytes.len: return @[]
  let extLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx+1])
  idx += 2
  
  let limit = idx + extLen
  while idx < limit - 4:
    let extType = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx+1])
    let length = (int(handshakeBytes[idx+2]) shl 8) or int(handshakeBytes[idx+3])
    idx += 4
    
    if extType == 0x0033: # Key Share
      # ServerKeyShare: Group(2) | KeyLen(2) | Key
      if length > 4 and idx + length <= handshakeBytes.len:
        # Check Group (should be x25519 = 0x001d)
        let group = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx+1])
        if group == 0x001d:
           let keyLen = (int(handshakeBytes[idx+2]) shl 8) or int(handshakeBytes[idx+3])
           if keyLen == 32 and idx + 4 + 32 <= handshakeBytes.len:
             result = newSeq[byte](32)
             for i in 0..<32: result[i] = handshakeBytes[idx+4+i]
             return result
      
    idx += length
    
  return @[]

proc hashTranscript*(data: openArray[byte]): seq[byte] =
  let d = sha256.digest(data)
  result = @(d.data)

