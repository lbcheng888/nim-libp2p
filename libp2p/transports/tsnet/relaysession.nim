{.push raises: [].}

import std/[strutils]
import nimcrypto

import ../../crypto/[chacha20poly1305, curve25519, hkdf]
import ../../utility

const
  TsnetRelaySessionKeySize* = 32
  TsnetRelaySessionNonceSize* = 12
  TsnetRelayInfoLabel = "nim-libp2p-tsnet-relay-v1"
  TsnetTypedPrivatePrefix = "privkey:"

type
  TsnetRelaySessionKey* = array[TsnetRelaySessionKeySize, byte]

proc decodeHexNibble(ch: char): int =
  case ch
  of '0' .. '9':
    ord(ch) - ord('0')
  of 'a' .. 'f':
    ord(ch) - ord('a') + 10
  of 'A' .. 'F':
    ord(ch) - ord('A') + 10
  else:
    -1

proc decodeHexBytes(text: string, expectedBytes: int): Result[seq[byte], string] =
  let raw = text.strip()
  if raw.len != expectedBytes * 2:
    return err("tsnet relay key material has invalid hex length")
  result = ok(newSeq[byte](expectedBytes))
  for i in 0 ..< expectedBytes:
    let hi = decodeHexNibble(raw[i * 2])
    let lo = decodeHexNibble(raw[i * 2 + 1])
    if hi < 0 or lo < 0:
      return err("tsnet relay key material contains invalid hex")
    result.get()[i] = byte((hi shl 4) or lo)

proc parsePrivateKey(text: string): Result[Curve25519Key, string] =
  let trimmed = text.strip()
  if not trimmed.startsWith(TsnetTypedPrivatePrefix):
    return err("tsnet relay private key is not a typed Curve25519 private key")
  let decoded = decodeHexBytes(
    trimmed.substr(TsnetTypedPrivatePrefix.len),
    Curve25519KeySize
  ).valueOr:
    return err(error)
  ok(intoCurve25519Key(decoded))

proc parsePublicKey(text: string): Result[Curve25519Key, string] =
  let trimmed = text.strip()
  let colon = trimmed.find(':')
  if colon <= 0 or colon + 1 >= trimmed.len:
    return err("tsnet relay public key is malformed")
  let decoded = decodeHexBytes(
    trimmed.substr(colon + 1),
    Curve25519KeySize
  ).valueOr:
    return err(error)
  ok(intoCurve25519Key(decoded))

proc deriveRelaySessionKey*(
    localPrivateKey: string,
    remotePublicKey: string,
    salt: openArray[byte] = [],
    info = TsnetRelayInfoLabel
): Result[TsnetRelaySessionKey, string] =
  let local = parsePrivateKey(localPrivateKey).valueOr:
    return err(error)
  var remote = parsePublicKey(remotePublicKey).valueOr:
    return err(error)
  Curve25519.mul(remote, local)
  var outputs: array[1, HkdfResult[TsnetRelaySessionKeySize]]
  sha256.hkdf(salt, remote, info.toOpenArrayByte(0, info.high), outputs)
  ok(outputs[0])

proc nonceForSequence(sequence: uint64): ChaChaPolyNonce =
  var raw: array[TsnetRelaySessionNonceSize, byte]
  raw[0] = byte('t')
  raw[1] = byte('s')
  raw[2] = byte('n')
  raw[3] = byte('r')
  for idx in 0 ..< 8:
    let shift = uint((7 - idx) * 8)
    raw[4 + idx] = byte((sequence shr shift) and 0xFF'u64)
  intoChaChaPolyNonce(raw)

proc encryptRelayPayload*(
    sessionKey: TsnetRelaySessionKey,
    sequence: uint64,
    payload: openArray[byte],
    aad: openArray[byte] = []
): Result[seq[byte], string] =
  var ciphertext = @payload
  var tag: ChaChaPolyTag
  ChaChaPoly.encrypt(
    intoChaChaPolyKey(sessionKey),
    nonceForSequence(sequence),
    tag,
    ciphertext,
    aad
  )
  ciphertext.add(tag)
  ok(ciphertext)

proc decryptRelayPayload*(
    sessionKey: TsnetRelaySessionKey,
    sequence: uint64,
    ciphertext: openArray[byte],
    aad: openArray[byte] = []
): Result[seq[byte], string] =
  if ciphertext.len < 16:
    return err("tsnet relay ciphertext is too short")
  let bodyLen = ciphertext.len - 16
  var body = newSeq[byte](bodyLen)
  for idx in 0 ..< bodyLen:
    body[idx] = ciphertext[idx]
  var tagBytes: array[16, byte]
  for idx in 0 ..< 16:
    tagBytes[idx] = ciphertext[bodyLen + idx]
  var tag = intoChaChaPolyTag(tagBytes)
  ChaChaPoly.decrypt(
    intoChaChaPolyKey(sessionKey),
    nonceForSequence(sequence),
    tag,
    body,
    aad
  )
  ok(body)
