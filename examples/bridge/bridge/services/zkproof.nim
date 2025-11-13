import std/[json, options]
import nimcrypto/sha2
import stew/byteutils
import bridge/model

const
  ProofSchemaVersion* = 1
  ProofTypeLockWitness* = "zkLockWitness"

type
  ZkProofEnvelope* = object
    schemaVersion*: int
    proofId*: string
    proofType*: string
    digest*: string
    payload*: JsonNode
    createdAt*: int64

proc digestPayload(payload: JsonNode): string =
  let bytes = ($payload).toBytes()
  sha256.digest(bytes).data.toHex()

proc enrichPayload(ev: LockEventMessage; payload: JsonNode): JsonNode =
  var enriched =
    if payload.isNil(): newJObject()
    else: payload.copy()
  enriched["eventId"] = %ev.eventId
  enriched["asset"] = %ev.asset
  enriched["amount"] = %ev.amount
  enriched["sourceChain"] = %ev.sourceChain
  enriched["targetChain"] = %ev.targetChain
  enriched["watcherPeer"] = %ev.watcherPeer
  enriched["proofKey"] = %ev.proofKey
  enriched

proc buildLockWitness*(ev: LockEventMessage; payload: JsonNode): ZkProofEnvelope =
  let normalized = enrichPayload(ev, payload)
  let digest = digestPayload(normalized)
  ZkProofEnvelope(
    schemaVersion: ProofSchemaVersion,
    proofId: if ev.proofKey.len > 0: ev.proofKey else: ev.eventId,
    proofType: ProofTypeLockWitness,
    digest: digest,
    payload: normalized,
    createdAt: nowEpoch(),
  )

proc encodeProof*(proof: ZkProofEnvelope): string =
  let node = %*{
    "schemaVersion": proof.schemaVersion,
    "proofId": proof.proofId,
    "proofType": proof.proofType,
    "digest": proof.digest,
    "createdAt": proof.createdAt,
    "payload": proof.payload,
  }
  $node

proc decodeProof*(blob: string): Option[ZkProofEnvelope] =
  if blob.len == 0:
    return none(ZkProofEnvelope)
  try:
    let node = parseJson(blob)
    if node.kind != JObject or not node.hasKey("payload"):
      return none(ZkProofEnvelope)
    let schemaVal =
      if node.hasKey("schemaVersion"): node["schemaVersion"].getInt()
      else: ProofSchemaVersion
    let proofId = if node.hasKey("proofId"): node["proofId"].getStr() else: ""
    let proofType =
      if node.hasKey("proofType"): node["proofType"].getStr()
      else: ProofTypeLockWitness
    let digest = if node.hasKey("digest"): node["digest"].getStr() else: ""
    let createdAt =
      if node.hasKey("createdAt"): node["createdAt"].getInt()
      else: nowEpoch()
    var payload = node["payload"]
    some(
      ZkProofEnvelope(
        schemaVersion: schemaVal,
        proofId: proofId,
        proofType: proofType,
        digest: digest,
        payload: payload,
        createdAt: createdAt,
      )
    )
  except CatchableError:
    none(ZkProofEnvelope)

proc verifyEnvelope*(proof: ZkProofEnvelope): bool =
  proof.digest.len > 0 and digestPayload(proof.payload) == proof.digest

proc verifyEventProof*(ev: LockEventMessage): bool =
  let proofOpt = decodeProof(ev.proofBlob)
  if proofOpt.isNone():
    return false
  let proof = proofOpt.get()
  if proof.proofType != ProofTypeLockWitness:
    return false
  if ev.proofKey.len > 0 and proof.proofId.len > 0 and proof.proofId != ev.proofKey:
    return false
  if ev.proofDigest.len > 0 and proof.digest.len > 0 and proof.digest != ev.proofDigest:
    return false
  verifyEnvelope(proof)

proc attachProof*(ev: var LockEventMessage; payload: JsonNode): ZkProofEnvelope =
  let proof = buildLockWitness(ev, payload)
  ev.proofBlob = encodeProof(proof)
  ev.proofDigest = proof.digest
  proof

proc proofPayload*(blob: string): Option[JsonNode] =
  let proofOpt = decodeProof(blob)
  if proofOpt.isNone():
    return none(JsonNode)
  some(proofOpt.get().payload)

proc eventProofPayload*(ev: LockEventMessage): Option[JsonNode] =
  proofPayload(ev.proofBlob)

proc shortDigest*(digest: string; limit = 16): string =
  if limit <= 0 or digest.len <= limit:
    digest
  else:
    digest[0 ..< limit]
