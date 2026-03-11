# Nim-LibP2P
# Copyright (c) 2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import std/[base64, strutils, times]
import stew/byteutils
import chronicles
import bearssl/rand
import results

import ../../crypto/crypto
import ../../crypto/ecnist
import ../../crypto/minasn1
import ../../errors
import ../../../libp2p/peerid

logScope:
  topics = "libp2p tls certificate"

type
  TLSCertificateError* = object of LPError
  KeyGenerationError* = object of TLSCertificateError
  CertificateCreationError* = object of TLSCertificateError
  CertificatePubKeySerializationError* = object of TLSCertificateError
  CertificateParsingError* = object of TLSCertificateError
  IdentityPubKeySerializationError* = object of TLSCertificateError
  IdentitySigningError* = object of TLSCertificateError

  P2pExtension* = object
    publicKey*: seq[byte]
    signature*: seq[byte]

  P2pCertificate* = object
    extension*: P2pExtension
    pubKeyDer: seq[byte]
    validFrom: Time
    validTo: Time

  CertificateX509* = object
    certificate*: seq[byte]
    privateKey*: seq[byte]

  EncodingFormat* = enum
    DER
    PEM

  DerNode = object
    tag: byte
    start: int
    valueStart: int
    valueLen: int

const
  Libp2pOid = [0x2B'u8, 0x06'u8, 0x01'u8, 0x04'u8, 0x01'u8, 0x83'u8, 0xA2'u8,
    0x5A'u8, 0x01'u8, 0x01'u8]
  CommonNameOid = [0x55'u8, 0x04'u8, 0x03'u8]
  EcdsaWithSha256Oid = [0x2A'u8, 0x86'u8, 0x48'u8, 0xCE'u8, 0x3D'u8, 0x04'u8,
    0x03'u8, 0x02'u8]

func publicKey*(cert: P2pCertificate): PublicKey =
  PublicKey.init(cert.extension.publicKey).get()

func peerId*(cert: P2pCertificate): PeerId =
  PeerId.init(cert.publicKey()).tryGet()

func makeSignatureMessage(pubKey: seq[byte]): seq[byte] {.inline.} =
  let prefix = "libp2p-tls-handshake:".toBytes()
  result = newSeq[byte](prefix.len + pubKey.len)
  if prefix.len > 0:
    copyMem(result[0].addr, unsafeAddr prefix[0], prefix.len)
  if pubKey.len > 0:
    copyMem(result[prefix.len].addr, unsafeAddr pubKey[0], pubKey.len)

func makeCommonName(identityKeyPair: KeyPair): string {.inline.} =
  try:
    $(PeerId.init(identityKeyPair.pubkey).tryGet())
  except LPError:
    raiseAssert "pubkey must be set"

proc encodeLength(length: int): seq[byte] =
  if length < 0:
    raise newException(CertificateCreationError, "negative DER length")
  if length < 0x80:
    return @[byte(length)]
  var tmp: seq[byte] = @[]
  var value = length
  while value > 0:
    tmp.add(byte(value and 0xFF))
    value = value shr 8
  result = @[byte(0x80 or tmp.len)]
  for i in countdown(tmp.high, 0):
    result.add(tmp[i])

proc wrapDer(tag: byte, payload: openArray[byte]): seq[byte] =
  result = @[tag]
  result.add(encodeLength(payload.len))
  for b in payload:
    result.add(b)

proc encodeIntegerBytes(value: openArray[byte]): seq[byte] =
  var tmp = newSeq[byte](16)
  let need = asn1EncodeInteger(tmp, value)
  result.setLen(need)
  discard asn1EncodeInteger(result, value)

proc encodeInteger(value: uint64): seq[byte] =
  var tmp = newSeq[byte](16)
  let need = asn1EncodeInteger(tmp, value)
  result.setLen(need)
  discard asn1EncodeInteger(result, value)

proc encodeOctetString(value: openArray[byte]): seq[byte] =
  result = wrapDer(0x04'u8, value)

proc encodeBitString(value: openArray[byte]): seq[byte] =
  var tmp = newSeq[byte](16 + value.len)
  let need = asn1EncodeBitString(tmp, value)
  result.setLen(need)
  discard asn1EncodeBitString(result, value)

proc encodeOid(value: openArray[byte]): seq[byte] =
  var tmp = newSeq[byte](16 + value.len)
  let need = asn1EncodeOid(tmp, value)
  result.setLen(need)
  discard asn1EncodeOid(result, value)

proc encodeSequence(parts: openArray[seq[byte]]): seq[byte] =
  var payload: seq[byte] = @[]
  for part in parts:
    payload.add(part)
  result = wrapDer(0x30'u8, payload)

proc encodeSet(parts: openArray[seq[byte]]): seq[byte] =
  var payload: seq[byte] = @[]
  for part in parts:
    payload.add(part)
  result = wrapDer(0x31'u8, payload)

proc encodeContext(tag: int, value: openArray[byte]): seq[byte] =
  if tag < 0 or tag > 15:
    raise newException(CertificateCreationError, "invalid context tag")
  result = wrapDer(byte(0xA0 or (tag and 0x0F)), value)

proc encodeUtf8String(value: string): seq[byte] =
  result = wrapDer(0x0C'u8, value.toBytes())

proc formatGeneralizedTime(value: Time): string =
  try:
    let fmt = initTimeFormat("yyyyMMddHHmmss")
    format(value.utc(), fmt) & "Z"
  except TimeFormatParseError as exc:
    raiseAssert "time format is constant: " & exc.msg

proc encodeGeneralizedTime(value: Time): seq[byte] =
  result = wrapDer(0x18'u8, formatGeneralizedTime(value).toBytes())

proc makeAlgorithmIdentifier(): seq[byte] =
  encodeSequence([encodeOid(EcdsaWithSha256Oid)])

proc makePkcs8PrivateKey(seckeyDer: openArray[byte]): seq[byte] =
  let algo = encodeSequence([encodeOid(Asn1OidEcPublicKey), encodeOid(Asn1OidSecp256r1)])
  encodeSequence([encodeInteger(0'u64), algo, encodeOctetString(seckeyDer)])

proc makeName(commonName: string): seq[byte] =
  let attr = encodeSequence([encodeOid(CommonNameOid), encodeUtf8String(commonName)])
  encodeSequence([encodeSet([attr])])

proc makeLibp2pExtension(identityPubKey, signature: openArray[byte]): seq[byte] =
  let inner = encodeSequence([encodeOctetString(identityPubKey), encodeOctetString(signature)])
  encodeSequence([encodeOid(Libp2pOid), encodeOctetString(inner)])

proc toPem(header: string, data: openArray[byte]): seq[byte] =
  let encoded = base64.encode(data)
  var text = "-----BEGIN " & header & "-----\n"
  var i = 0
  while i < encoded.len:
    let j = min(i + 64, encoded.len)
    text.add(encoded[i ..< j])
    text.add('\n')
    i = j
  text.add("-----END " & header & "-----\n")
  text.toBytes()

proc bytesToString(data: openArray[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc makeExtValues(
    identityKeyPair: KeyPair, certPubKeyDer: seq[byte]
): tuple[signature: seq[byte], pubkey: seq[byte]] {.
    raises: [
      CertificatePubKeySerializationError, IdentitySigningError,
      IdentityPubKeySerializationError,
    ]
.} =
  let msg = makeSignatureMessage(certPubKeyDer)
  let signatureResult = identityKeyPair.seckey.sign(msg)
  if signatureResult.isErr:
    raise newException(
      IdentitySigningError, "Failed to sign the message with the identity key"
    )
  let pubKeyBytesResult = identityKeyPair.pubkey.getBytes()
  if pubKeyBytesResult.isErr:
    raise newException(
      IdentityPubKeySerializationError, "Failed to get identity public key bytes"
    )
  (signatureResult.get().data, pubKeyBytesResult.get())

proc makeSerial(rng: ref HmacDrbgContext): seq[byte] =
  result = newSeq[byte](20)
  rng[].generate(result)
  var allZero = true
  for b in result:
    if b != 0'u8:
      allZero = false
      break
  if allZero and result.len > 0:
    result[^1] = 1'u8

proc parseDecimal(certTime: string, start, count: int): int {.raises: [TimeParseError].} =
  if start < 0 or count <= 0 or start + count > certTime.len:
    raise newException(TimeParseError, "invalid time range")
  var value = 0
  for i in start ..< start + count:
    let ch = certTime[i]
    if ch < '0' or ch > '9':
      raise newException(TimeParseError, "invalid decimal time")
    value = value * 10 + (ord(ch) - ord('0'))
  value

proc parseCertTime*(certTime: string): Time {.raises: [TimeParseError].} =
  if certTime.endsWith("Z"):
    if certTime.len == 15:
      let year = parseDecimal(certTime, 0, 4)
      let month = parseDecimal(certTime, 4, 2)
      let day = parseDecimal(certTime, 6, 2)
      let hour = parseDecimal(certTime, 8, 2)
      let minute = parseDecimal(certTime, 10, 2)
      let second = parseDecimal(certTime, 12, 2)
      return dateTime(year, Month(month), day, hour, minute, second, 0, utc()).toTime()
    if certTime.len == 13:
      var year = parseDecimal(certTime, 0, 2)
      year = if year < 50: year + 2000 else: year + 1900
      let month = parseDecimal(certTime, 2, 2)
      let day = parseDecimal(certTime, 4, 2)
      let hour = parseDecimal(certTime, 6, 2)
      let minute = parseDecimal(certTime, 8, 2)
      let second = parseDecimal(certTime, 10, 2)
      return dateTime(year, Month(month), day, hour, minute, second, 0, utc()).toTime()
    raise newException(TimeParseError, "unsupported ASN.1 time format")

  var timeNoZone = certTime
  if timeNoZone.len >= 4:
    timeNoZone = timeNoZone[0 ..^ 5]
  timeNoZone = timeNoZone.replace("  ", " ")
  const certTimeFormat = "MMM d hh:mm:ss yyyy"
  const f = initTimeFormat(certTimeFormat)
  parse(timeNoZone, f, utc()).toTime()

proc readNode(data: openArray[byte], pos: int): DerNode {.raises: [CertificateParsingError].} =
  if pos < 0 or pos >= data.len:
    raise newException(CertificateParsingError, "DER node out of bounds")
  let start = pos
  let tag = data[pos]
  var offset = pos + 1
  if offset >= data.len:
    raise newException(CertificateParsingError, "truncated DER length")
  let firstLen = data[offset]
  inc offset
  var valueLen = 0
  if (firstLen and 0x80'u8) == 0:
    valueLen = int(firstLen)
  else:
    let octets = int(firstLen and 0x7F'u8)
    if octets == 0 or octets > 8 or offset + octets > data.len:
      raise newException(CertificateParsingError, "invalid DER long length")
    for i in 0 ..< octets:
      valueLen = (valueLen shl 8) or int(data[offset + i])
    offset += octets
  if valueLen < 0 or offset + valueLen > data.len:
    raise newException(CertificateParsingError, "DER value overruns buffer")
  DerNode(tag: tag, start: start, valueStart: offset, valueLen: valueLen)

func nodeEnd(node: DerNode): int {.inline.} =
  node.valueStart + node.valueLen

proc nodeValue(data: openArray[byte], node: DerNode): seq[byte] =
  if node.valueLen == 0:
    return @[]
  result = newSeq[byte](node.valueLen)
  copyMem(result[0].addr, unsafeAddr data[node.valueStart], node.valueLen)

proc nodeFull(data: openArray[byte], node: DerNode): seq[byte] =
  let length = nodeEnd(node) - node.start
  if length == 0:
    return @[]
  result = newSeq[byte](length)
  copyMem(result[0].addr, unsafeAddr data[node.start], length)

proc childNodes(
    data: openArray[byte], node: DerNode
): seq[DerNode] {.raises: [CertificateParsingError].} =
  var offset = node.valueStart
  let finish = nodeEnd(node)
  while offset < finish:
    let child = readNode(data, offset)
    result.add(child)
    offset = nodeEnd(child)
  if offset != finish:
    raise newException(CertificateParsingError, "DER composite did not terminate cleanly")

proc parseLibp2pExtension(
    data: openArray[byte], extValue: DerNode
): P2pExtension {.raises: [CertificateParsingError].} =
  if extValue.tag != 0x04'u8:
    raise newException(CertificateParsingError, "libp2p extension is not an octet string")
  let innerBytes = nodeValue(data, extValue)
  let inner = readNode(innerBytes, 0)
  if inner.tag != 0x30'u8:
    raise newException(CertificateParsingError, "libp2p extension payload is not a sequence")
  let elements = childNodes(innerBytes, inner)
  if elements.len != 2 or elements[0].tag != 0x04'u8 or elements[1].tag != 0x04'u8:
    raise newException(CertificateParsingError, "libp2p extension payload is malformed")
  P2pExtension(
    publicKey: nodeValue(innerBytes, elements[0]),
    signature: nodeValue(innerBytes, elements[1]),
  )

proc parseExtensions(
    data: openArray[byte], extCtx: DerNode
): P2pExtension {.raises: [CertificateParsingError].} =
  if extCtx.tag != 0xA3'u8:
    raise newException(CertificateParsingError, "expected extension context tag")
  let seqNode = readNode(data, extCtx.valueStart)
  if seqNode.tag != 0x30'u8:
    raise newException(CertificateParsingError, "extension container is not a sequence")
  for ext in childNodes(data, seqNode):
    let fields = childNodes(data, ext)
    if fields.len < 2:
      continue
    let oidField = fields[0]
    if oidField.tag != 0x06'u8 or nodeValue(data, oidField) != @Libp2pOid:
      continue
    let valueField = fields[^1]
    return parseLibp2pExtension(data, valueField)
  raise newException(CertificateParsingError, "libp2p extension not found")

proc generateX509*(
    identityKeyPair: KeyPair,
    validFrom: Time = fromUnix(157813200),
    validTo: Time = fromUnix(67090165200),
    encodingFormat: EncodingFormat = EncodingFormat.DER,
): CertificateX509 {.
    raises: [
      KeyGenerationError, IdentitySigningError, IdentityPubKeySerializationError,
      CertificateCreationError, CertificatePubKeySerializationError,
    ]
.} =
  let rng = newRng()
  if rng.isNil:
    raise newException(KeyGenerationError, "Failed to initialize RNG")

  let certKeyPair = EcKeyPair.random(Secp256r1, rng[]).valueOr:
    raise newException(KeyGenerationError, "Failed to generate certificate key")

  let certPubKeyDer = certKeyPair.pubkey.getBytes().valueOr:
    raise newException(
      CertificatePubKeySerializationError,
      "Failed to serialize the certificate pubkey",
    )
  let certPrivKeyDer = certKeyPair.seckey.getBytes().valueOr:
    raise newException(KeyGenerationError, "Failed to serialize certificate private key")

  let extValues = makeExtValues(identityKeyPair, certPubKeyDer)
  let name = makeName(makeCommonName(identityKeyPair))
  let algo = makeAlgorithmIdentifier()
  let validity = encodeSequence([
    encodeGeneralizedTime(validFrom),
    encodeGeneralizedTime(validTo),
  ])
  let extensions = encodeContext(
    3,
    encodeSequence([makeLibp2pExtension(extValues.pubkey, extValues.signature)]),
  )
  let tbs = encodeSequence([
    encodeContext(0, encodeInteger(2'u64)),
    encodeIntegerBytes(makeSerial(rng)),
    algo,
    name,
    validity,
    name,
    certPubKeyDer,
    extensions,
  ])
  let certSignature = certKeyPair.seckey.sign(tbs).valueOr:
    raise newException(CertificateCreationError, "Failed to sign certificate")
  let certSignatureDer = certSignature.getBytes().valueOr:
    raise newException(CertificateCreationError, "Failed to encode certificate signature")
  let certDer = encodeSequence([tbs, algo, encodeBitString(certSignatureDer)])
  let pkcs8Der = makePkcs8PrivateKey(certPrivKeyDer)

  case encodingFormat
  of DER:
    CertificateX509(certificate: certDer, privateKey: pkcs8Der)
  of PEM:
    CertificateX509(
      certificate: toPem("CERTIFICATE", certDer),
      privateKey: toPem("PRIVATE KEY", pkcs8Der),
    )

proc parse*(
    certificateDer: seq[byte]
): P2pCertificate {.raises: [CertificateParsingError].} =
  let root = readNode(certificateDer, 0)
  if root.tag != 0x30'u8 or nodeEnd(root) != certificateDer.len:
    raise newException(CertificateParsingError, "certificate is not a DER SEQUENCE")
  let certFields = childNodes(certificateDer, root)
  if certFields.len < 1:
    raise newException(CertificateParsingError, "certificate missing TBSCertificate")
  let tbs = certFields[0]
  if tbs.tag != 0x30'u8:
    raise newException(CertificateParsingError, "TBSCertificate is not a sequence")
  let tbsFields = childNodes(certificateDer, tbs)
  if tbsFields.len < 7:
    raise newException(CertificateParsingError, "TBSCertificate too short")

  var index = 0
  if tbsFields[0].tag == 0xA0'u8:
    inc index
  if tbsFields.len < index + 6:
    raise newException(CertificateParsingError, "TBSCertificate missing required fields")

  let validityNode = tbsFields[index + 3]
  if validityNode.tag != 0x30'u8:
    raise newException(CertificateParsingError, "certificate validity is not a sequence")
  let validityFields = childNodes(certificateDer, validityNode)
  if validityFields.len != 2:
    raise newException(CertificateParsingError, "certificate validity is malformed")
  let validFrom =
    try:
      parseCertTime(bytesToString(nodeValue(certificateDer, validityFields[0])))
    except TimeParseError as exc:
      raise newException(
        CertificateParsingError,
        "Failed to parse certificate validity time: " & exc.msg,
        exc,
      )
  let validTo =
    try:
      parseCertTime(bytesToString(nodeValue(certificateDer, validityFields[1])))
    except TimeParseError as exc:
      raise newException(
        CertificateParsingError,
        "Failed to parse certificate validity time: " & exc.msg,
        exc,
      )

  let spkiNode = tbsFields[index + 5]
  if spkiNode.tag != 0x30'u8:
    raise newException(CertificateParsingError, "certificate public key is malformed")

  var extensionNode: DerNode
  var foundExtension = false
  for i in index + 6 ..< tbsFields.len:
    if tbsFields[i].tag == 0xA3'u8:
      extensionNode = tbsFields[i]
      foundExtension = true
      break
  if not foundExtension:
    raise newException(CertificateParsingError, "certificate missing libp2p extension")

  P2pCertificate(
    extension: parseExtensions(certificateDer, extensionNode),
    pubKeyDer: nodeFull(certificateDer, spkiNode),
    validFrom: validFrom,
    validTo: validTo,
  )

proc verify*(self: P2pCertificate): bool =
  let currentTime = now().utc().toTime()
  if not (currentTime >= self.validFrom and currentTime < self.validTo):
    return false

  var sig: Signature
  var key: PublicKey
  if sig.init(self.extension.signature) and key.init(self.extension.publicKey):
    let msg = makeSignatureMessage(self.pubKeyDer)
    return sig.verify(msg, key)

  false
