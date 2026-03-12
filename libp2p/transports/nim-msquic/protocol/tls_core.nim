## RFC 9001 TLS Core for QUIC Initial Packet Protection
## Handles Key Derivation (HKDF), Header Protection, and Payload Encryption.

import std/sequtils
import std/strutils
import std/endians
import nimcrypto/rijndael
import bearssl/aead
import bearssl/blockx
import bearssl/kdf
import bearssl/hash as bhash
import bearssl/hash
import bearssl/hmac as bhmac
import libp2p/crypto/hkdf
import libp2p/crypto/curve25519
import nimcrypto/sysrand

type
  Secret = seq[byte]
  Iv = seq[byte]
  Key = seq[byte]
  HpKey = seq[byte]

  HandshakeMessage* = object
    msgType*: byte
    raw*: seq[byte]
    body*: seq[byte]

  QuicTransportParameters* = object
    present*: bool
    maxIdleTimeoutMs*: uint64
    maxUdpPayloadSize*: uint64
    initialMaxData*: uint64
    initialMaxStreamDataBidiLocal*: uint64
    initialMaxStreamDataBidiRemote*: uint64
    initialMaxStreamDataUni*: uint64
    initialMaxStreamsBidi*: uint64
    initialMaxStreamsUni*: uint64
    ackDelayExponent*: uint64
    maxAckDelayMs*: uint64
    activeConnectionIdLimit*: uint64
    disableActiveMigration*: bool
    initialSourceConnectionId*: seq[byte]
  
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
  HandshakeTypeClientHello* = 0x01'u8
  HandshakeTypeNewSessionTicket* = 0x04'u8
  HandshakeTypeServerHello* = 0x02'u8
  HandshakeTypeFinished* = 0x14'u8
  ExtQuicTransportParameters = 0x0039'u16
  ExtBuiltinResumptionTicket = 0xFFA0'u16
  ExtBuiltinResumeAccepted = 0xFFA1'u16
  ExtBuiltinResumptionBinder = 0xFFA2'u16
  ExtBuiltinZeroRttAccepted = 0xFFA3'u16
  ExtBuiltinTicketAge = 0xFFA4'u16
  ExtTlsEarlyData = 0x002A'u16

  TpMaxIdleTimeout = 0x01'u64
  TpMaxUdpPayloadSize = 0x03'u64
  TpInitialMaxData = 0x04'u64
  TpInitialMaxStreamDataBidiLocal = 0x05'u64
  TpInitialMaxStreamDataBidiRemote = 0x06'u64
  TpInitialMaxStreamDataUni = 0x07'u64
  TpInitialMaxStreamsBidi = 0x08'u64
  TpInitialMaxStreamsUni = 0x09'u64
  TpAckDelayExponent = 0x0A'u64
  TpMaxAckDelay = 0x0B'u64
  TpDisableActiveMigration = 0x0C'u64
  TpActiveConnectionIdLimit = 0x0E'u64
  TpInitialSourceConnectionId = 0x0F'u64

proc appendVarInt(buf: var seq[byte]; val: uint64) =
  if val <= 63:
    buf.add(byte(val))
  elif val <= 16383:
    var tmp = uint16(val) or 0x4000'u16
    var outVal: uint16
    bigEndian16(addr outVal, addr tmp)
    buf.add(cast[ptr array[2, byte]](addr outVal)[])
  elif val <= 1073741823:
    var tmp = uint32(val) or 0x80000000'u32
    var outVal: uint32
    bigEndian32(addr outVal, addr tmp)
    buf.add(cast[ptr array[4, byte]](addr outVal)[])
  else:
    var tmp = val or 0xC000000000000000'u64
    var outVal: uint64
    bigEndian64(addr outVal, addr tmp)
    buf.add(cast[ptr array[8, byte]](addr outVal)[])

proc readVarIntAt(data: openArray[byte]; pos: var int): uint64 =
  if pos >= data.len:
    return 0'u64
  let first = data[pos]
  let prefix = first shr 6
  let length =
    case prefix
    of 0'u8: 1
    of 1'u8: 2
    of 2'u8: 4
    else: 8
  if pos + length > data.len:
    pos = data.len
    return 0'u64
  case length
  of 1:
    result = uint64(first and 0x3F'u8)
  of 2:
    result = (uint64(first and 0x3F'u8) shl 8) or uint64(data[pos + 1])
  of 4:
    result = (uint64(first and 0x3F'u8) shl 24) or
      (uint64(data[pos + 1]) shl 16) or
      (uint64(data[pos + 2]) shl 8) or
      uint64(data[pos + 3])
  else:
    result = (uint64(first and 0x3F'u8) shl 56) or
      (uint64(data[pos + 1]) shl 48) or
      (uint64(data[pos + 2]) shl 40) or
      (uint64(data[pos + 3]) shl 32) or
      (uint64(data[pos + 4]) shl 24) or
      (uint64(data[pos + 5]) shl 16) or
      (uint64(data[pos + 6]) shl 8) or
      uint64(data[pos + 7])
  pos += length

proc encodeHandshakeMessage(msgType: byte; body: openArray[byte]): seq[byte] =
  result = @[msgType,
    byte((body.len shr 16) and 0xFF),
    byte((body.len shr 8) and 0xFF),
    byte(body.len and 0xFF)]
  result.add(body)

proc appendUint16BE(buf: var seq[byte]; value: uint16) =
  var outVal: uint16
  var host = value
  bigEndian16(addr outVal, addr host)
  buf.add(cast[ptr array[2, byte]](addr outVal)[])

proc appendUint32BE(buf: var seq[byte]; value: uint32) =
  var outVal: uint32
  var host = value
  bigEndian32(addr outVal, addr host)
  buf.add(cast[ptr array[4, byte]](addr outVal)[])

proc readUint16BE(data: openArray[byte]; pos: var int): uint16 =
  if pos + 2 > data.len:
    return 0'u16
  result = (uint16(data[pos]) shl 8) or uint16(data[pos + 1])
  pos += 2

proc readUint32BE(data: openArray[byte]; pos: var int): uint32 =
  if pos + 4 > data.len:
    return 0'u32
  result = (uint32(data[pos]) shl 24) or
    (uint32(data[pos + 1]) shl 16) or
    (uint32(data[pos + 2]) shl 8) or
    uint32(data[pos + 3])
  pos += 4

proc appendExtension(extBuf: var seq[byte]; extType: uint16; payload: openArray[byte]) =
  extBuf.appendUint16BE(extType)
  extBuf.appendUint16BE(uint16(min(payload.len, high(uint16).int)))
  extBuf.add(payload[0 ..< min(payload.len, high(uint16).int)])

type
  NewSessionTicket* = object
    present*: bool
    ticketLifetimeSec*: uint32
    ticketAgeAdd*: uint32
    maxEarlyData*: uint32
    ticket*: seq[byte]

  BuiltinResumptionOffer* = object
    ticket*: seq[byte]
    binder*: seq[byte]
    agePresent*: bool
    obfuscatedTicketAge*: uint32
    earlyDataRequested*: bool

  ZeroRttMaterial* = object
    key*: seq[byte]
    iv*: seq[byte]
    hp*: seq[byte]

# --- Initial Secrets ---

# Helper for wrapping BearSSL-based SHA-256/HMAC-SHA256.
proc sha256Digest(data: openArray[byte]): array[32, byte] =
  var ctx: bhash.Sha256Context
  bhash.sha256Init(ctx)
  if data.len > 0:
    bhash.sha224Update(ctx, unsafeAddr data[0], uint(data.len))
  bhash.sha256Out(ctx, addr result[0])

proc hmacSha256(key, data: openArray[byte]): array[32, byte] =
  var keyCtx: bhmac.HmacKeyContext
  bhmac.hmacKeyInit(
    keyCtx,
    addr bhash.sha256Vtable,
    if key.len > 0: unsafeAddr key[0] else: nil,
    uint(key.len)
  )
  var ctx: bhmac.HmacContext
  bhmac.hmacInit(ctx, keyCtx, 32)
  if data.len > 0:
    bhmac.hmacUpdate(ctx, unsafeAddr data[0], uint(data.len))
  discard bhmac.hmacOut(ctx, addr result[0])

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
    t = @digest
    okm.add(t)
    
  for i in 0 ..< Len:
    result[i] = okm[i]

proc deriveSecretWithContext[Len: static int](secret: openArray[byte];
    label: string; context: openArray[byte]): array[Len, byte] =
  var hkdfLabel: seq[byte] = @[]
  var lenNet: uint16
  var lenHost = uint16(Len)
  bigEndian16(addr lenNet, addr lenHost)
  hkdfLabel.add(cast[ptr array[2, byte]](addr lenNet)[])

  let fullLabel = LabelPrefix & label
  hkdfLabel.add(byte(fullLabel.len))
  for c in fullLabel:
    hkdfLabel.add(byte(c))

  hkdfLabel.add(byte(context.len))
  hkdfLabel.add(context)
  hkdfExpand[Len](secret, hkdfLabel)

proc deriveSecret[Len: static int](secret: openArray[byte], label: string): array[Len, byte] =
  deriveSecretWithContext[Len](secret, label, [])

proc defaultQuicTransportParameters*(initialSourceConnectionId: openArray[byte]): QuicTransportParameters =
  result.present = true
  result.maxIdleTimeoutMs = 30_000'u64
  result.maxUdpPayloadSize = 1_200'u64
  result.initialMaxData = 1_048_576'u64
  result.initialMaxStreamDataBidiLocal = 262_144'u64
  result.initialMaxStreamDataBidiRemote = 262_144'u64
  result.initialMaxStreamDataUni = 262_144'u64
  result.initialMaxStreamsBidi = 64'u64
  result.initialMaxStreamsUni = 64'u64
  result.ackDelayExponent = 3'u64
  result.maxAckDelayMs = 25'u64
  result.activeConnectionIdLimit = 4'u64
  result.disableActiveMigration = false
  result.initialSourceConnectionId = @initialSourceConnectionId

proc appendTransportParamValue(buf: var seq[byte]; paramId, value: uint64) =
  var encodedValue: seq[byte] = @[]
  appendVarInt(encodedValue, value)
  appendVarInt(buf, paramId)
  appendVarInt(buf, uint64(encodedValue.len))
  buf.add(encodedValue)

proc encodeQuicTransportParameters*(params: QuicTransportParameters): seq[byte] =
  appendTransportParamValue(result, TpMaxIdleTimeout, params.maxIdleTimeoutMs)
  appendTransportParamValue(result, TpMaxUdpPayloadSize, params.maxUdpPayloadSize)
  appendTransportParamValue(result, TpInitialMaxData, params.initialMaxData)
  appendTransportParamValue(result, TpInitialMaxStreamDataBidiLocal, params.initialMaxStreamDataBidiLocal)
  appendTransportParamValue(result, TpInitialMaxStreamDataBidiRemote, params.initialMaxStreamDataBidiRemote)
  appendTransportParamValue(result, TpInitialMaxStreamDataUni, params.initialMaxStreamDataUni)
  appendTransportParamValue(result, TpInitialMaxStreamsBidi, params.initialMaxStreamsBidi)
  appendTransportParamValue(result, TpInitialMaxStreamsUni, params.initialMaxStreamsUni)
  appendTransportParamValue(result, TpAckDelayExponent, params.ackDelayExponent)
  appendTransportParamValue(result, TpMaxAckDelay, params.maxAckDelayMs)
  appendTransportParamValue(result, TpActiveConnectionIdLimit, params.activeConnectionIdLimit)
  if params.disableActiveMigration:
    appendVarInt(result, TpDisableActiveMigration)
    appendVarInt(result, 0'u64)
  if params.initialSourceConnectionId.len > 0:
    appendVarInt(result, TpInitialSourceConnectionId)
    appendVarInt(result, uint64(params.initialSourceConnectionId.len))
    result.add(params.initialSourceConnectionId)

proc transportParametersEqual*(lhs, rhs: QuicTransportParameters): bool =
  lhs.present == rhs.present and
    lhs.maxIdleTimeoutMs == rhs.maxIdleTimeoutMs and
    lhs.maxUdpPayloadSize == rhs.maxUdpPayloadSize and
    lhs.initialMaxData == rhs.initialMaxData and
    lhs.initialMaxStreamDataBidiLocal == rhs.initialMaxStreamDataBidiLocal and
    lhs.initialMaxStreamDataBidiRemote == rhs.initialMaxStreamDataBidiRemote and
    lhs.initialMaxStreamDataUni == rhs.initialMaxStreamDataUni and
    lhs.initialMaxStreamsBidi == rhs.initialMaxStreamsBidi and
    lhs.initialMaxStreamsUni == rhs.initialMaxStreamsUni and
    lhs.ackDelayExponent == rhs.ackDelayExponent and
    lhs.maxAckDelayMs == rhs.maxAckDelayMs and
    lhs.activeConnectionIdLimit == rhs.activeConnectionIdLimit and
    lhs.disableActiveMigration == rhs.disableActiveMigration and
    lhs.initialSourceConnectionId == rhs.initialSourceConnectionId

proc deriveInitialSecrets*(dcid: openArray[byte]): InitialSecrets =
  # 1. Initial Secret = HKDF-Extract(InitialSalt, DCID)
  # We use generic HKDF Extract here (HMAC-SHA256(Salt, IKM))
  # Since we don't have a direct Extract func, we use HMAC directly.
  let initialSecretDigest = hmacSha256(InitialSaltV1, dcid)
  let initialSecret = initialSecretDigest
  
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
    
  var ciphertext = newSeq[byte](payload.len)
  var ctrCtx: AesCtCtrKeys
  aesCtCtrInit(ctrCtx, unsafeAddr key[0], uint(key.len))
  var gcmCtx: GcmContext
  gcmInit(gcmCtx, cast[ptr ptr BlockCtrClass](addr ctrCtx), ghashCtmul32)
  gcmReset(gcmCtx, unsafeAddr nonce[0], uint(nonce.len))
  if header.len > 0:
    gcmAadInject(gcmCtx, unsafeAddr header[0], uint(header.len))
  gcmFlip(gcmCtx)
  if payload.len > 0:
    copyMem(addr ciphertext[0], unsafeAddr payload[0], payload.len)
    gcmRun(gcmCtx, 1, addr ciphertext[0], uint(ciphertext.len))

  gcmGetTag(gcmCtx, addr paramTag[0])
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
    
  var plaintext = newSeq[byte](ciphertext.len)
  var ctrCtx: AesCtCtrKeys
  aesCtCtrInit(ctrCtx, unsafeAddr key[0], uint(key.len))
  var gcmCtx: GcmContext
  gcmInit(gcmCtx, cast[ptr ptr BlockCtrClass](addr ctrCtx), ghashCtmul32)
  gcmReset(gcmCtx, unsafeAddr nonce[0], uint(nonce.len))
  if header.len > 0:
    gcmAadInject(gcmCtx, unsafeAddr header[0], uint(header.len))
  gcmFlip(gcmCtx)
  if ciphertext.len > 0:
    copyMem(addr plaintext[0], unsafeAddr ciphertext[0], ciphertext.len)
    gcmRun(gcmCtx, 0, addr plaintext[0], uint(plaintext.len))

  if tag.len < 16 or gcmCheckTag(gcmCtx, unsafeAddr tag[0]) == 0'u32:
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

proc splitHandshakeMessages*(data: openArray[byte]): seq[HandshakeMessage] =
  var pos = 0
  while pos + 4 <= data.len:
    let msgType = data[pos]
    let length = (int(data[pos + 1]) shl 16) or (int(data[pos + 2]) shl 8) or int(data[pos + 3])
    let endPos = pos + 4 + length
    if endPos > data.len:
      break
    result.add(HandshakeMessage(
      msgType: msgType,
      raw: @(data[pos ..< endPos]),
      body: @(data[pos + 4 ..< endPos])
    ))
    pos = endPos

proc extractExtensionPayload(handshakeBytes: openArray[byte]; extTypeTarget: uint16): seq[byte] =
  var idx = 0
  if handshakeBytes.len < 4:
    return @[]
  let msgType = handshakeBytes[0]
  idx += 4
  case msgType
  of HandshakeTypeClientHello:
    idx += 34
    if idx >= handshakeBytes.len:
      return @[]
    let sessIdLen = int(handshakeBytes[idx])
    idx += 1 + sessIdLen
    if idx + 2 > handshakeBytes.len:
      return @[]
    let cipherLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
    idx += 2 + cipherLen
    if idx >= handshakeBytes.len:
      return @[]
    let compressionLen = int(handshakeBytes[idx])
    idx += 1 + compressionLen
  of HandshakeTypeServerHello:
    idx += 34
    if idx >= handshakeBytes.len:
      return @[]
    let sessIdLen = int(handshakeBytes[idx])
    idx += 1 + sessIdLen
    idx += 3
  else:
    return @[]

  if idx + 2 > handshakeBytes.len:
    return @[]
  let extLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
  idx += 2
  let limit = min(handshakeBytes.len, idx + extLen)
  while idx + 4 <= limit:
    let extType = (uint16(handshakeBytes[idx]) shl 8) or uint16(handshakeBytes[idx + 1])
    let length = (int(handshakeBytes[idx + 2]) shl 8) or int(handshakeBytes[idx + 3])
    idx += 4
    if idx + length > limit:
      return @[]
    if extType == extTypeTarget:
      return @(handshakeBytes[idx ..< idx + length])
    idx += length
  @[]

proc hasExtension(handshakeBytes: openArray[byte]; extTypeTarget: uint16): bool =
  var idx = 0
  if handshakeBytes.len < 4:
    return false
  let msgType = handshakeBytes[0]
  idx += 4
  case msgType
  of HandshakeTypeClientHello:
    idx += 34
    if idx >= handshakeBytes.len:
      return false
    let sessIdLen = int(handshakeBytes[idx])
    idx += 1 + sessIdLen
    if idx + 2 > handshakeBytes.len:
      return false
    let cipherLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
    idx += 2 + cipherLen
    if idx >= handshakeBytes.len:
      return false
    let compressionLen = int(handshakeBytes[idx])
    idx += 1 + compressionLen
  of HandshakeTypeServerHello:
    idx += 34
    if idx >= handshakeBytes.len:
      return false
    let sessIdLen = int(handshakeBytes[idx])
    idx += 1 + sessIdLen
    idx += 3
  else:
    return false

  if idx + 2 > handshakeBytes.len:
    return false
  let extLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
  idx += 2
  let limit = min(handshakeBytes.len, idx + extLen)
  while idx + 4 <= limit:
    let extType = (uint16(handshakeBytes[idx]) shl 8) or uint16(handshakeBytes[idx + 1])
    let length = (int(handshakeBytes[idx + 2]) shl 8) or int(handshakeBytes[idx + 3])
    idx += 4
    if idx + length > limit:
      return false
    if extType == extTypeTarget:
      return true
    idx += length
  false

proc stripExtension(handshakeBytes: openArray[byte]; extTypeTarget: uint16): seq[byte] =
  result = @handshakeBytes
  if handshakeBytes.len < 4:
    return
  var idx = 4
  case handshakeBytes[0]
  of HandshakeTypeClientHello:
    idx += 34
    if idx >= handshakeBytes.len:
      return
    let sessIdLen = int(handshakeBytes[idx])
    idx += 1 + sessIdLen
    if idx + 2 > handshakeBytes.len:
      return
    let cipherLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
    idx += 2 + cipherLen
    if idx >= handshakeBytes.len:
      return
    let compressionLen = int(handshakeBytes[idx])
    idx += 1 + compressionLen
  of HandshakeTypeServerHello:
    idx += 34
    if idx >= handshakeBytes.len:
      return
    let sessIdLen = int(handshakeBytes[idx])
    idx += 1 + sessIdLen
    idx += 3
  else:
    return

  if idx + 2 > handshakeBytes.len:
    return
  let extLenPos = idx
  let extLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
  idx += 2
  let limit = min(handshakeBytes.len, idx + extLen)
  var rebuiltExts: seq[byte] = @[]
  while idx + 4 <= limit:
    let extHeaderStart = idx
    let extType = (uint16(handshakeBytes[idx]) shl 8) or uint16(handshakeBytes[idx + 1])
    let length = (int(handshakeBytes[idx + 2]) shl 8) or int(handshakeBytes[idx + 3])
    idx += 4
    if idx + length > limit:
      return @handshakeBytes
    if extType != extTypeTarget:
      rebuiltExts.add(handshakeBytes[extHeaderStart ..< idx + length])
    idx += length
  result.setLen(extLenPos + 2)
  result.add(rebuiltExts)
  let newExtLen = rebuiltExts.len
  result[extLenPos] = byte((newExtLen shr 8) and 0xFF)
  result[extLenPos + 1] = byte(newExtLen and 0xFF)
  let msgLen = result.len - 4
  result[1] = byte((msgLen shr 16) and 0xFF)
  result[2] = byte((msgLen shr 8) and 0xFF)
  result[3] = byte(msgLen and 0xFF)

proc parseQuicTransportParameters*(handshakeBytes: openArray[byte]): QuicTransportParameters =
  let payload = extractExtensionPayload(handshakeBytes, ExtQuicTransportParameters)
  if payload.len == 0:
    return
  result.present = true
  var pos = 0
  while pos < payload.len:
    let paramId = readVarIntAt(payload, pos)
    let paramLen = int(readVarIntAt(payload, pos))
    if pos + paramLen > payload.len:
      result.present = false
      return
    case paramId
    of TpDisableActiveMigration:
      result.disableActiveMigration = true
    of TpInitialSourceConnectionId:
      result.initialSourceConnectionId = @(payload[pos ..< pos + paramLen])
    else:
      var innerPos = pos
      let value = readVarIntAt(payload, innerPos)
      if innerPos == pos + paramLen:
        case paramId
        of TpMaxIdleTimeout: result.maxIdleTimeoutMs = value
        of TpMaxUdpPayloadSize: result.maxUdpPayloadSize = value
        of TpInitialMaxData: result.initialMaxData = value
        of TpInitialMaxStreamDataBidiLocal: result.initialMaxStreamDataBidiLocal = value
        of TpInitialMaxStreamDataBidiRemote: result.initialMaxStreamDataBidiRemote = value
        of TpInitialMaxStreamDataUni: result.initialMaxStreamDataUni = value
        of TpInitialMaxStreamsBidi: result.initialMaxStreamsBidi = value
        of TpInitialMaxStreamsUni: result.initialMaxStreamsUni = value
        of TpAckDelayExponent: result.ackDelayExponent = value
        of TpMaxAckDelay: result.maxAckDelayMs = value
        of TpActiveConnectionIdLimit: result.activeConnectionIdLimit = value
        else: discard
    pos += paramLen

proc encodeFinished*(baseSecret: openArray[byte]; transcriptHash: openArray[byte]): seq[byte] =
  let finishedKey = deriveSecretWithContext[32](baseSecret, "finished", [])
  let verifyData = hmacSha256(finishedKey, transcriptHash)
  encodeHandshakeMessage(HandshakeTypeFinished, verifyData)

proc verifyFinished*(baseSecret: openArray[byte]; transcriptHash: openArray[byte];
    finishedMessage: openArray[byte]): bool =
  let messages = splitHandshakeMessages(finishedMessage)
  if messages.len != 1 or messages[0].msgType != HandshakeTypeFinished:
    return false
  let expected = encodeFinished(baseSecret, transcriptHash)
  if expected.len != finishedMessage.len:
    return false
  var diff = 0'u8
  for idx in 0 ..< expected.len:
    diff = diff or (expected[idx] xor finishedMessage[idx])
  diff == 0'u8

proc extractClientResumptionTicket*(clientHelloBytes: openArray[byte]): seq[byte] =
  extractExtensionPayload(clientHelloBytes, ExtBuiltinResumptionTicket)

proc extractClientResumptionBinder*(clientHelloBytes: openArray[byte]): seq[byte] =
  extractExtensionPayload(clientHelloBytes, ExtBuiltinResumptionBinder)

proc extractClientResumptionTicketAge*(clientHelloBytes: openArray[byte];
    obfuscatedAge: var uint32): bool =
  let payload = extractExtensionPayload(clientHelloBytes, ExtBuiltinTicketAge)
  if payload.len != 4:
    return false
  var pos = 0
  obfuscatedAge = readUint32BE(payload, pos)
  true

proc clientHelloRequestsEarlyData*(clientHelloBytes: openArray[byte]): bool =
  hasExtension(clientHelloBytes, ExtTlsEarlyData)

proc serverHelloSessionResumed*(serverHelloBytes: openArray[byte]): bool =
  let payload = extractExtensionPayload(serverHelloBytes, ExtBuiltinResumeAccepted)
  payload.len > 0 and payload[0] == 1'u8

proc serverHelloZeroRttAccepted*(serverHelloBytes: openArray[byte]): bool =
  let payload = extractExtensionPayload(serverHelloBytes, ExtBuiltinZeroRttAccepted)
  payload.len > 0 and payload[0] == 1'u8

proc deriveBuiltinBinderKey(ticket: openArray[byte]): array[32, byte] =
  let pskSeed = sha256Digest(ticket)
  deriveSecretWithContext[32](pskSeed, "builtin binder", [])

proc computeBuiltinResumptionBinder*(clientHelloWithoutBinder: openArray[byte];
    ticket: openArray[byte]): seq[byte] =
  if ticket.len == 0:
    return @[]
  let binderKey = deriveBuiltinBinderKey(ticket)
  let transcriptHash = sha256Digest(clientHelloWithoutBinder)
  let binder = hmacSha256(binderKey, transcriptHash)
  @binder

proc verifyBuiltinResumptionBinder*(clientHelloBytes: openArray[byte];
    ticket: openArray[byte]; binder: openArray[byte]): bool =
  if ticket.len == 0 or binder.len == 0:
    return false
  let stripped = stripExtension(clientHelloBytes, ExtBuiltinResumptionBinder)
  let expected = computeBuiltinResumptionBinder(stripped, ticket)
  if expected.len != binder.len:
    return false
  var diff = 0'u8
  for idx in 0 ..< expected.len:
    diff = diff or (expected[idx] xor binder[idx])
  diff == 0'u8

proc extractBuiltinResumptionOffer*(clientHelloBytes: openArray[byte]): BuiltinResumptionOffer =
  result.ticket = extractClientResumptionTicket(clientHelloBytes)
  result.binder = extractClientResumptionBinder(clientHelloBytes)
  result.agePresent = extractClientResumptionTicketAge(
    clientHelloBytes,
    result.obfuscatedTicketAge
  )
  result.earlyDataRequested = clientHelloRequestsEarlyData(clientHelloBytes)

proc encodeNewSessionTicket*(ticket: openArray[byte];
    maxEarlyData: uint32 = 16_384'u32;
    ticketAgeAdd: uint32 = 0'u32;
    ticketLifetimeSec: uint32 = 600'u32): seq[byte] =
  var body: seq[byte] = @[]
  body.appendUint32BE(ticketLifetimeSec)
  body.appendUint32BE(ticketAgeAdd)
  body.add(0'u8)             # ticket_nonce len
  body.appendUint16BE(uint16(min(ticket.len, high(uint16).int)))
  body.add(ticket[0 ..< min(ticket.len, high(uint16).int)])

  var extBuf: seq[byte] = @[]
  if maxEarlyData > 0'u32:
    var earlyDataPayload: seq[byte] = @[]
    earlyDataPayload.appendUint32BE(maxEarlyData)
    appendExtension(extBuf, ExtTlsEarlyData, earlyDataPayload)

  body.appendUint16BE(uint16(extBuf.len))
  body.add(extBuf)
  encodeHandshakeMessage(HandshakeTypeNewSessionTicket, body)

proc parseNewSessionTicket*(ticketMessage: openArray[byte]): NewSessionTicket =
  let messages = splitHandshakeMessages(ticketMessage)
  if messages.len != 1 or messages[0].msgType != HandshakeTypeNewSessionTicket:
    return
  let body = messages[0].body
  if body.len < 4 + 4 + 1 + 2 + 2:
    return
  var pos = 0
  result.present = true
  result.ticketLifetimeSec = readUint32BE(body, pos)
  result.ticketAgeAdd = readUint32BE(body, pos)
  let nonceLen = int(body[pos])
  inc pos
  if pos + nonceLen + 2 > body.len:
    result.present = false
    return
  pos += nonceLen
  let ticketLen = int(readUint16BE(body, pos))
  if pos + ticketLen + 2 > body.len:
    result.present = false
    return
  result.ticket = @(body[pos ..< pos + ticketLen])
  pos += ticketLen
  let extLen = int(readUint16BE(body, pos))
  if pos + extLen > body.len:
    result.present = false
    return
  let extLimit = pos + extLen
  while pos + 4 <= extLimit:
    let extType = readUint16BE(body, pos)
    let payloadLen = int(readUint16BE(body, pos))
    if pos + payloadLen > extLimit:
      result.present = false
      return
    if extType == ExtTlsEarlyData and payloadLen == 4:
      var extPos = pos
      result.maxEarlyData = readUint32BE(body, extPos)
    pos += payloadLen

proc deriveBuiltinZeroRttMaterial*(ticket: openArray[byte]): ZeroRttMaterial =
  if ticket.len == 0:
    return
  let pskSeed = sha256Digest(ticket)
  let zeroRttSecret = deriveSecretWithContext[32](pskSeed, "builtin zrt", [])
  let key = deriveSecret[16](zeroRttSecret, "quic key")
  let iv = deriveSecret[12](zeroRttSecret, "quic iv")
  let hp = deriveSecret[16](zeroRttSecret, "quic hp")
  result.key = @key
  result.iv = @iv
  result.hp = @hp

proc encodeClientHelloBody(initialSourceCid: openArray[byte], keyShare: ClientKeyShare;
    transportParams: QuicTransportParameters; randomBytesValue: openArray[byte];
    resumptionTicket: seq[byte]; binderPayload: seq[byte];
    requestEarlyData = true; includeTicketAge = false;
    obfuscatedTicketAge = 0'u32): seq[byte] =
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
  if randomBytesValue.len == 32:
    buf.add(randomBytesValue)
  else:
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
  
  let tpPayload =
    if transportParams.present:
      encodeQuicTransportParameters(transportParams)
    else:
      encodeQuicTransportParameters(defaultQuicTransportParameters(initialSourceCid))
  appendExtension(extBuf, ExtQuicTransportParameters, tpPayload)
  if resumptionTicket.len > 0:
    appendExtension(extBuf, ExtBuiltinResumptionTicket, resumptionTicket)
    if includeTicketAge:
      var ticketAgePayload: seq[byte] = @[]
      ticketAgePayload.appendUint32BE(obfuscatedTicketAge)
      appendExtension(extBuf, ExtBuiltinTicketAge, ticketAgePayload)
    if requestEarlyData:
      appendExtension(extBuf, ExtTlsEarlyData, [])
  if binderPayload.len > 0:
    appendExtension(extBuf, ExtBuiltinResumptionBinder, binderPayload)

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

proc encodeClientHello*(initialSourceCid: openArray[byte], keyShare: ClientKeyShare;
    transportParams: QuicTransportParameters; resumptionTicket: seq[byte] = @[];
    requestEarlyData = true; includeTicketAge = false;
    obfuscatedTicketAge = 0'u32): seq[byte] =
  var clientRandom: array[32, byte]
  discard randomBytes(clientRandom)
  result = encodeClientHelloBody(
    initialSourceCid,
    keyShare,
    transportParams,
    clientRandom,
    resumptionTicket,
    @[],
    requestEarlyData,
    includeTicketAge,
    obfuscatedTicketAge
  )
  if resumptionTicket.len > 0:
    let binder = computeBuiltinResumptionBinder(result, resumptionTicket)
    result = encodeClientHelloBody(
      initialSourceCid,
      keyShare,
      transportParams,
      clientRandom,
      resumptionTicket,
      binder,
      requestEarlyData,
      includeTicketAge,
      obfuscatedTicketAge
    )

proc encodeClientHelloWithBuiltinResumptionOverride*(initialSourceCid: openArray[byte];
    keyShare: ClientKeyShare; transportParams: QuicTransportParameters;
    binderSourceTicket: seq[byte]; ticketPayload: seq[byte];
    tamperBinder = false; requestEarlyData = true;
    includeTicketAge = false; obfuscatedTicketAge = 0'u32): seq[byte] =
  var clientRandom: array[32, byte]
  discard randomBytes(clientRandom)
  result = encodeClientHelloBody(
    initialSourceCid,
    keyShare,
    transportParams,
    clientRandom,
    ticketPayload,
    @[],
    requestEarlyData,
    includeTicketAge,
    obfuscatedTicketAge
  )
  if binderSourceTicket.len > 0:
    var binder = computeBuiltinResumptionBinder(result, binderSourceTicket)
    if tamperBinder and binder.len > 0:
      binder[0] = binder[0] xor 0x5A'u8
    result = encodeClientHelloBody(
      initialSourceCid,
      keyShare,
      transportParams,
      clientRandom,
      ticketPayload,
      binder,
      requestEarlyData,
      includeTicketAge,
      obfuscatedTicketAge
    )

proc encodeServerHello*(keySharePublic: openArray[byte];
    transportParams: QuicTransportParameters; sessionResumed = false;
    zeroRttAccepted = false): seq[byte] =
  ## Minimal TLS 1.3 ServerHello for the builtin QUIC path.
  var buf: seq[byte] = @[]

  buf.add(0x02'u8)
  buf.add([0x00'u8, 0x00, 0x00])
  let startOffset = buf.len

  buf.add([0x03'u8, 0x03])

  var random: array[32, byte]
  discard randomBytes(random)
  buf.add(random)

  buf.add(0x00'u8) # legacy_session_id_echo length
  buf.add([0x13'u8, 0x01]) # TLS_AES_128_GCM_SHA256
  buf.add(0x00'u8) # legacy compression method

  var extBuf: seq[byte] = @[]

  extBuf.add([0x00'u8, 0x2b, 0x00, 0x02, 0x03, 0x04]) # supported_versions = TLS1.3

  var ksExt: seq[byte] = @[]
  ksExt.add([0x00'u8, 0x1d]) # x25519
  ksExt.add([0x00'u8, 0x20]) # 32-byte key
  ksExt.add(keySharePublic)
  extBuf.add([0x00'u8, 0x33])
  var ksExtLenBe: uint16
  var ksExtLenHost = uint16(ksExt.len)
  bigEndian16(addr ksExtLenBe, addr ksExtLenHost)
  extBuf.add(cast[ptr array[2, byte]](addr ksExtLenBe)[])
  extBuf.add(ksExt)

  let tpPayload =
    if transportParams.present:
      encodeQuicTransportParameters(transportParams)
    else:
      encodeQuicTransportParameters(defaultQuicTransportParameters(@[]))
  appendExtension(extBuf, ExtQuicTransportParameters, tpPayload)
  if sessionResumed:
    appendExtension(extBuf, ExtBuiltinResumeAccepted, @[1'u8])
  if zeroRttAccepted:
    appendExtension(extBuf, ExtBuiltinZeroRttAccepted, @[1'u8])

  var extLenBe: uint16
  var extLenHost = uint16(extBuf.len)
  bigEndian16(addr extLenBe, addr extLenHost)
  buf.add(cast[ptr array[2, byte]](addr extLenBe)[])
  buf.add(extBuf)

  let msgLen = buf.len - startOffset
  buf[1] = byte((msgLen shr 16) and 0xff)
  buf[2] = byte((msgLen shr 8) and 0xff)
  buf[3] = byte(msgLen and 0xff)

  buf

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
  let earlySecret = earlySecretDigest

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
  
  let derivedSecretArr = deriveSecretWithContext[32](earlySecret, "derived", emptyHash)
  let derivedSecret = @derivedSecretArr
  
  # C. Handshake Secret = HKDF-Extract(Derived, SharedSecret)
  let handshakeSecretDigest = hmacSha256(derivedSecret, sharedSecret)
  let handshakeSecret = handshakeSecretDigest
  
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

  let derivedSecretArr = deriveSecretWithContext[32](handshakeSecret, "derived", emptyHash)
  let derivedSecret = @derivedSecretArr
  
  let zeroArr = [0'u8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
  let masterSecretDigest = hmacSha256(derivedSecret, zeroArr)
  let masterSecret = masterSecretDigest
  
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

proc findClientKeyShare*(handshakeBytes: openArray[byte]): seq[byte] =
  ## Find the x25519 key share from a minimal ClientHello.
  var idx = 0
  if handshakeBytes.len < 4:
    return @[]

  let msgType = handshakeBytes[0]
  if msgType != 1:
    return @[]

  idx += 4
  idx += 34 # legacy_version + random

  if idx >= handshakeBytes.len:
    return @[]
  let sessIdLen = int(handshakeBytes[idx])
  idx += 1 + sessIdLen

  if idx + 2 > handshakeBytes.len:
    return @[]
  let cipherLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
  idx += 2 + cipherLen

  if idx >= handshakeBytes.len:
    return @[]
  let compressionLen = int(handshakeBytes[idx])
  idx += 1 + compressionLen

  if idx + 2 > handshakeBytes.len:
    return @[]
  let extLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
  idx += 2

  let limit = min(handshakeBytes.len, idx + extLen)
  while idx + 4 <= limit:
    let extType = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
    let length = (int(handshakeBytes[idx + 2]) shl 8) or int(handshakeBytes[idx + 3])
    idx += 4
    if idx + length > limit:
      return @[]

    if extType == 0x0033 and length >= 6:
      let listLen = (int(handshakeBytes[idx]) shl 8) or int(handshakeBytes[idx + 1])
      var cursor = idx + 2
      let listLimit = min(idx + 2 + listLen, idx + length)
      while cursor + 4 <= listLimit:
        let group = (int(handshakeBytes[cursor]) shl 8) or int(handshakeBytes[cursor + 1])
        let keyLen = (int(handshakeBytes[cursor + 2]) shl 8) or int(handshakeBytes[cursor + 3])
        cursor += 4
        if cursor + keyLen > listLimit:
          break
        if group == 0x001d and keyLen == 32:
          result = newSeq[byte](32)
          for i in 0 ..< 32:
            result[i] = handshakeBytes[cursor + i]
          return result
        cursor += keyLen

    idx += length

  @[]

proc hashTranscript*(data: openArray[byte]): seq[byte] =
  result = @(sha256Digest(data))
