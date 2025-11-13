# Nim-Libp2p CoinJoin shuffle tests

import unittest2, results, std/sequtils
import libp2p/crypto/coinjoin

when defined(libp2p_coinjoin):
  proc sampleCommitment(amountVal: uint64, seedVal: uint64): CoinJoinCommitment =
    let amount = encodeAmount(amountVal)
    let blind = hkdfBlind(@[byte seedVal], amountVal, seedVal).valueOr:
      raise newException(Exception, $error)
    pedersenCommit(amount, blind).valueOr:
      raise newException(Exception, $error)

suite "CoinJoin shuffle prototype":
  test "proveShuffle handles empty inputs":
    let res = proveShuffle(@[], @[])
    if coinJoinSupportEnabled():
      check res.isErr and res.error.kind == cjInvalidInput
    else:
      check res.isErr and res.error.kind == cjDisabled

  test "verifyShuffle rejects length mismatch":
    let res = verifyShuffle(
      ShuffleProof(challenge: default(CoinJoinPoint), responses: @[]),
      @[CoinJoinCommitment()],
      @[]
    )
    check res.isErr

  when defined(libp2p_coinjoin):
    test "shuffle verify succeeds for permutation":
      let inputs = @[sampleCommitment(5, 1), sampleCommitment(7, 2), sampleCommitment(11, 3)]
      let outputs = inputs.reversed
      let proof = proveShuffle(inputs, outputs).valueOr:
        fail $error
        return
      check verifyShuffle(proof, inputs, outputs).valueOr(false)

    test "shuffle detect mismatch":
      let inputs = @[sampleCommitment(5, 1), sampleCommitment(7, 2)]
      let outputs = @[sampleCommitment(7, 2), sampleCommitment(13, 4)]
      let res = proveShuffle(inputs, outputs)
      check res.isErr and res.error.kind == cjInvalidInput
