import std/[base64, json, os]
import nimcrypto/utils

import ../peerid
import ../crypto/crypto

import ./codec
import ./types

type
  NodeIdentity* = object
    privateKey*: PrivateKey
    publicKey*: PublicKey
    peerId*: PeerId
    account*: string

proc stringFromBytes(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc accountFromPublicKey*(pubKey: PublicKey): string =
  let bytes = pubKey.getBytes().tryGet()
  hashHex(bytes)[0 .. 39]

proc newNodeIdentity*(privateKey: PrivateKey): NodeIdentity =
  let publicKey = privateKey.getPublicKey().tryGet()
  let peerId = PeerId.init(privateKey).tryGet()
  NodeIdentity(
    privateKey: privateKey,
    publicKey: publicKey,
    peerId: peerId,
    account: accountFromPublicKey(publicKey),
  )

proc newNodeIdentity*(): NodeIdentity =
  let rng = newRng()
  let privateKey = PrivateKey.random(PKScheme.Ed25519, rng[]).tryGet()
  newNodeIdentity(privateKey)

proc encodePrivateKeyHex*(key: PrivateKey): string =
  toHex(key.getBytes().tryGet())

proc encodePublicKeyHex*(key: PublicKey): string =
  toHex(key.getBytes().tryGet())

proc peerIdString*(peerId: PeerId): string =
  $peerId

proc signBytes*(privateKey: PrivateKey, payload: openArray[byte]): string =
  $(privateKey.sign(payload).tryGet())

proc verifyBytes*(publicKey: PublicKey, payload: openArray[byte], signatureHex: string): bool =
  let sig = Signature.init(signatureHex).valueOr:
    return false
  sig.verify(payload, publicKey)

proc signTx*(identity: NodeIdentity, tx: var Tx) =
  tx.sender = identity.account
  tx.senderPublicKey = encodePublicKeyHex(identity.publicKey)
  tx.txId = computeTxId(tx)
  tx.signature = signBytes(identity.privateKey, txSigningBytes(tx))

proc verifyTx*(tx: Tx): bool =
  if tx.senderPublicKey.len == 0 or tx.signature.len == 0:
    return false
  let publicKey = PublicKey.init(tx.senderPublicKey).valueOr:
    return false
  if accountFromPublicKey(publicKey) != tx.sender:
    return false
  if computeTxId(tx) != tx.txId:
    return false
  verifyBytes(publicKey, txSigningBytes(tx), tx.signature)

proc signBatch*(identity: NodeIdentity, batch: var Batch) =
  batch.proposer = identity.account
  batch.proposerPublicKey = encodePublicKeyHex(identity.publicKey)
  batch.batchId = computeBatchId(batch)
  batch.signature = signBytes(identity.privateKey, batchSigningBytes(batch))

proc verifyBatch*(batch: Batch): bool =
  if batch.proposerPublicKey.len == 0 or batch.signature.len == 0:
    return false
  let publicKey = PublicKey.init(batch.proposerPublicKey).valueOr:
    return false
  if accountFromPublicKey(publicKey) != batch.proposer:
    return false
  if computeBatchId(batch) != batch.batchId:
    return false
  verifyBytes(publicKey, batchSigningBytes(batch), batch.signature)

proc encodeIdentityJson*(identity: NodeIdentity): JsonNode =
  result = newJObject()
  result["account"] = %identity.account
  result["peerId"] = %peerIdString(identity.peerId)
  result["privateKeyHex"] = %encodePrivateKeyHex(identity.privateKey)
  result["publicKeyHex"] = %encodePublicKeyHex(identity.publicKey)
  result["privateKeyBase64"] = %encode(stringFromBytes(identity.privateKey.getBytes().tryGet()))
  result["publicKeyBase64"] = %encode(stringFromBytes(identity.publicKey.getBytes().tryGet()))

proc saveIdentity*(path: string, identity: NodeIdentity) =
  createDir(parentDir(path))
  writeFile(path, $encodeIdentityJson(identity))

proc loadIdentity*(path: string): NodeIdentity =
  let payload = parseJson(readFile(path))
  let privateKey = PrivateKey.init(payload["privateKeyHex"].getStr()).tryGet()
  newNodeIdentity(privateKey)
