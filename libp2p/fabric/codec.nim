import std/[algorithm, json, jsonutils]

import ../rwad/codec
import ./types

export hashHex, encodeObj, decodeObj

proc clockCmp*(a, b: GanzhiClock): int =
  result = cmp(a.era, b.era)
  if result == 0:
    result = cmp(a.tick, b.tick)

proc `<`*(a, b: GanzhiClock): bool =
  clockCmp(a, b) < 0

proc `<=`*(a, b: GanzhiClock): bool =
  clockCmp(a, b) <= 0

proc maxClock*(a, b: GanzhiClock): GanzhiClock =
  if a <= b: b else: a

proc nextClock*(parent: GanzhiClock): GanzhiClock =
  if parent.tick < 59'u8:
    GanzhiClock(era: parent.era, tick: parent.tick + 1)
  else:
    GanzhiClock(era: parent.era + 1, tick: 0)

proc sortedStrings(items: openArray[string]): seq[string] =
  result = @items
  result.sort(system.cmp[string])

proc eventSigningNode*(event: FabricEvent): JsonNode =
  result = newJObject()
  result["threadId"] = toJson(event.threadId)
  result["author"] = %event.author
  result["authorPublicKey"] = %event.authorPublicKey
  result["authorPeerId"] = %event.authorPeerId
  result["contractRoot"] = %event.contractRoot
  result["entrypoint"] = %event.entrypoint
  result["args"] = event.args
  result["legacyTxId"] = %event.legacyTx.txId
  result["participants"] = %sortedStrings(event.participants)
  result["participantRoutes"] = toJson(event.participantRoutes)
  result["witnessRoutes"] = toJson(event.witnessRoutes)
  result["witnessSet"] = %sortedStrings(event.witnessSet)
  result["parents"] = %sortedStrings(event.parents)
  result["clock"] = toJson(event.clock)
  result["routingPath"] = toJson(event.routingPath)
  result["createdAt"] = %event.createdAt

proc computeEventId*(event: FabricEvent): string =
  hashHex($eventSigningNode(event))

proc attestationSigningNode*(att: EventAttestation): JsonNode =
  %*{
    "eventId": att.eventId,
    "signer": att.signer,
    "signerPublicKey": att.signerPublicKey,
    "signerPeerId": att.signerPeerId,
    "role": $att.role,
    "weight": att.weight,
    "issuedAt": att.issuedAt,
  }

proc attestationSigningBytes*(att: EventAttestation): seq[byte] =
  let payload = $attestationSigningNode(att)
  result = newSeq[byte](payload.len)
  if payload.len > 0:
    copyMem(addr result[0], unsafeAddr payload[0], payload.len)

proc computeEventCertificateId*(cert: EventCertificate): string =
  hashHex(encodeObj(cert))

proc computeAvoProposalId*(proposal: AvoProposal): string =
  hashHex(encodeObj(proposal.bundle) & ":" & $proposal.checkpointStartEra &
    ":" & $proposal.checkpointEndEra & ":" & proposal.proposer)

proc computeIsolationId*(record: IsolationRecord): string =
  hashHex(record.eventId & ":" & $record.kind & ":" & record.reason)

proc checkpointCandidateSigningNode*(candidate: CheckpointCandidate): JsonNode =
  result = newJObject()
  result["era"] = %candidate.era
  result["frontierDigest"] = %candidate.frontierDigest
  result["projectionRoots"] = toJson(candidate.projectionRoots)
  result["activeContractRoots"] = %sortedStrings(candidate.activeContractRoots)
  result["activeAvoSet"] = %sortedStrings(candidate.activeAvoSet)
  result["adoptedProposalIds"] = %sortedStrings(candidate.adoptedProposalIds)
  result["isolatedEventIds"] = %sortedStrings(candidate.isolatedEventIds)
  result["snapshotRoot"] = %candidate.snapshotRoot
  result["validatorSetRoot"] = %candidate.validatorSetRoot
  result["createdAt"] = %candidate.createdAt

proc computeCheckpointCandidateId*(candidate: CheckpointCandidate): string =
  hashHex($checkpointCandidateSigningNode(candidate))

proc checkpointVoteSigningNode*(vote: CheckpointVote): JsonNode =
  %*{
    "candidateId": vote.candidateId,
    "validator": vote.validator,
    "validatorPublicKey": vote.validatorPublicKey,
    "validatorPeerId": vote.validatorPeerId,
    "weight": vote.weight,
    "issuedAt": vote.issuedAt,
  }

proc checkpointVoteSigningBytes*(vote: CheckpointVote): seq[byte] =
  let payload = $checkpointVoteSigningNode(vote)
  result = newSeq[byte](payload.len)
  if payload.len > 0:
    copyMem(addr result[0], unsafeAddr payload[0], payload.len)

proc computeCheckpointId*(cert: CheckpointCertificate): string =
  hashHex(encodeObj(cert))
