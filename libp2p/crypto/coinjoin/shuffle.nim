# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[tables, sequtils]
import nimcrypto/sha2

import ./types
import ./secp_utils

proc commitmentsHash(commits: seq[CoinJoinCommitment], salt: string): CoinJoinPoint =
  var ctx = newSeq[byte]()
  ctx.appendString("coinjoin-shuffle-" & salt)
  for c in commits:
    ctx.add(c.point)
  let digest = sha256.digest(ctx)
  var point: CoinJoinPoint
  point[0] = 0x02
  for i in 1 ..< CoinJoinPoint.len:
    point[i] = digest.data[i - 1]
  point

proc pointsToCounts(commits: seq[CoinJoinCommitment]): CountTable[string] =
  result = initCountTable[string]()
  for c in commits:
    result.inc(string(c.point))

proc isPermutation(inputs, outputs: seq[CoinJoinCommitment]): bool =
  pointsToCounts(inputs) == pointsToCounts(outputs)

proc proveShuffle*(
    inputs, outputs: seq[CoinJoinCommitment]
): CoinJoinResult[ShuffleProof] =
  if not coinJoinSupportEnabled():
    return errDisabled[ShuffleProof]("proveShuffle")
  if inputs.len == 0 or outputs.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "shuffle requires non-empty inputs"))
  if inputs.len != outputs.len:
    return err(newCoinJoinError(cjInvalidInput, "shuffle inputs/outputs length mismatch"))
  if not isPermutation(inputs, outputs):
    return err(newCoinJoinError(cjInvalidInput, "outputs must be a permutation of inputs"))

  let inputHash = commitmentsHash(inputs, "in")
  let outputHash = commitmentsHash(outputs, "out")
  var proof = ShuffleProof()
  proof.challenge = commitmentsHash(inputs & outputs, "challenge")
  proof.responses = @[inputHash, outputHash]
  ok(proof)

proc verifyShuffle*(
    proof: ShuffleProof, inputs, outputs: seq[CoinJoinCommitment]
): CoinJoinResult[bool] =
  if not coinJoinSupportEnabled():
    return errDisabled[bool]("verifyShuffle")
  if inputs.len == 0 or outputs.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "shuffle requires non-empty inputs"))
  if inputs.len != outputs.len:
    return err(newCoinJoinError(cjInvalidInput, "shuffle inputs/outputs length mismatch"))
  if proof.responses.len != 2:
    return err(newCoinJoinError(cjInvalidInput, "shuffle proof missing hashes"))
  let expectedChallenge = commitmentsHash(inputs & outputs, "challenge")
  if expectedChallenge != proof.challenge:
    return ok(false)
  let expectedInputHash = commitmentsHash(inputs, "in")
  let expectedOutputHash = commitmentsHash(outputs, "out")
  if proof.responses[0] != expectedInputHash or proof.responses[1] != expectedOutputHash:
    return ok(false)
  ok(isPermutation(inputs, outputs))
