import std/[json, jsonutils]

import ../rwad/codec
import ../rwad/execution/state

type
  PolarSegment* = object
    name*: string
    payload*: JsonNode

  PolarSnapshotEnvelope* = object
    snapshotRoot*: string
    radial*: seq[PolarSegment]
    angular*: seq[PolarSegment]

proc buildEnvelope(snapshot: ChainStateSnapshot): PolarSnapshotEnvelope =
  result = PolarSnapshotEnvelope(
    snapshotRoot: hashHex(encodeObj(snapshot)),
    radial: @[
      PolarSegment(name: "params", payload: toJson(snapshot.params)),
      PolarSegment(name: "ledger", payload: %*{
        "totalRwadSupply": snapshot.totalRwadSupply,
        "nonces": snapshot.nonces,
        "rwadBalances": snapshot.rwadBalances,
      }),
      PolarSegment(name: "validators", payload: toJson(snapshot.validators)),
      PolarSegment(name: "contents", payload: toJson(snapshot.contents)),
      PolarSegment(name: "names", payload: toJson(snapshot.nameRecords)),
    ],
    angular: @[
      PolarSegment(name: "delegations", payload: toJson(snapshot.delegations)),
      PolarSegment(name: "reports", payload: toJson(snapshot.reports)),
      PolarSegment(name: "moderationCases", payload: toJson(snapshot.moderationCases)),
      PolarSegment(name: "tombstones", payload: toJson(snapshot.tombstones)),
      PolarSegment(name: "creditPositions", payload: toJson(snapshot.creditPositions)),
      PolarSegment(name: "mintIntents", payload: toJson(snapshot.mintIntents)),
      PolarSegment(name: "executionReceipts", payload: toJson(snapshot.executionReceipts)),
      PolarSegment(name: "custodySnapshots", payload: toJson(snapshot.custodySnapshots)),
      PolarSegment(name: "reserveProofs", payload: toJson(snapshot.reserveProofs)),
    ],
  )

proc encodePolarSnapshot*(snapshot: ChainStateSnapshot): seq[byte] =
  let payload = encodeObj(buildEnvelope(snapshot))
  result = newSeq[byte](payload.len)
  if payload.len > 0:
    copyMem(addr result[0], unsafeAddr payload[0], payload.len)

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc decodePolarSnapshot*(data: openArray[byte]): ChainStateSnapshot =
  let envelope = decodeObj[PolarSnapshotEnvelope](stringOf(data))
  var node = newJObject()
  for segment in envelope.radial:
    node[segment.name] = segment.payload
  for segment in envelope.angular:
    node[segment.name] = segment.payload
  result = ChainStateSnapshot(
    params: jsonTo(node["params"], typeof(result.params)),
    height: 0,
    epoch: 0,
    lastBlockId: "",
    stateRoot: "",
    totalRwadSupply: node["ledger"]["totalRwadSupply"].getInt().uint64,
    nonces: jsonTo(node["ledger"]["nonces"], typeof(result.nonces)),
    rwadBalances: jsonTo(node["ledger"]["rwadBalances"], typeof(result.rwadBalances)),
    validators: jsonTo(node["validators"], typeof(result.validators)),
    delegations: jsonTo(node["delegations"], typeof(result.delegations)),
    contents: jsonTo(node["contents"], typeof(result.contents)),
    reports: jsonTo(node["reports"], typeof(result.reports)),
    moderationCases: jsonTo(node["moderationCases"], typeof(result.moderationCases)),
    tombstones: jsonTo(node["tombstones"], typeof(result.tombstones)),
    creditPositions: jsonTo(node["creditPositions"], typeof(result.creditPositions)),
    mintIntents: jsonTo(node["mintIntents"], typeof(result.mintIntents)),
    executionReceipts: jsonTo(node["executionReceipts"], typeof(result.executionReceipts)),
    custodySnapshots: jsonTo(node["custodySnapshots"], typeof(result.custodySnapshots)),
    reserveProofs: jsonTo(node["reserveProofs"], typeof(result.reserveProofs)),
    nameRecords: jsonTo(node["names"], typeof(result.nameRecords)),
  )
  result.stateRoot = hashHex(encodeObj(result))

proc computePolarSnapshotRoot*(snapshot: ChainStateSnapshot): string =
  hashHex(encodePolarSnapshot(snapshot))
