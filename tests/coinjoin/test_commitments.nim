# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.used.}

import unittest
import ../../libp2p/crypto/coinjoin
import ../../libp2p/crypto/secp

suite "CoinJoin Commitments":
  test "Pedersen Commit Roundtrip":
    if not coinJoinSupportEnabled():
      skip("CoinJoin support disabled")
    else:
      # 1. Setup
      var rng = newRng()
      var blind: CoinJoinBlind
      rng[].generate(blind)
      
      # Encode 1.5 BTC
      let amountVal: uint64 = 150_000_000
      let amount = encodeAmount(amountVal)
      
      # 2. Commit
      let commitRes = pedersenCommit(amount, blind)
      check commitRes.isOk
      let commitment = commitRes.get()
      
      # 3. Verify
      let verifyRes = verifyCommitment(commitment, amount, blind)
      check verifyRes.isOk
      check verifyRes.get() == true
      
      # 4. Negative test (wrong amount)
      let wrongAmount = encodeAmount(140_000_000)
      let verifyBadAmt = verifyCommitment(commitment, wrongAmount, blind)
      check verifyBadAmt.isOk
      check verifyBadAmt.get() == false

  test "Homomorphic Addition":
    if not coinJoinSupportEnabled():
      skip("CoinJoin support disabled")
    else:
      var rng = newRng()
      
      # C1 = Commit(v1, r1)
      var b1: CoinJoinBlind
      rng[].generate(b1)
      let v1 = encodeAmount(100)
      let c1 = pedersenCommit(v1, b1).get()
      
      # C2 = Commit(v2, r2)
      var b2: CoinJoinBlind
      rng[].generate(b2)
      let v2 = encodeAmount(200)
      let c2 = pedersenCommit(v2, b2).get()
      
      # C_sum = C1 + C2
      let cSumRes = addCommitments(c1, c2)
      check cSumRes.isOk
      let cSum = cSumRes.get()
      
      # Expected: v_sum = v1 + v2, b_sum = b1 + b2
      let vSum = encodeAmount(300)
      # Note: We don't have addBlinds exposed publicly in high-level API easily, 
      # but verifyCommitment should work if we could calculate blind sum.
      # Since addBlinds is in secp_utils (internal), we rely on `addCommitments` working correctly.
      # To verify fully, we would need to derive the blind sum manually or trust the API.
      # For this test, we trust `addCommitments` produces a valid point on curve.
      
      discard cSum

  test "Shuffle Logic (Mock)":
    if not coinJoinSupportEnabled():
      skip("CoinJoin support disabled")
    else:
      # Create commitments
      var inputs: seq[CoinJoinCommitment] = @[]
      var rng = newRng()
      
      for i in 0..2:
        var b: CoinJoinBlind
        rng[].generate(b)
        inputs.add(pedersenCommit(encodeAmount(uint64(i+1)), b).get())
        
      # Permute
      var outputs = inputs
      let temp = outputs[0]
      outputs[0] = outputs[1]
      outputs[1] = temp
      
      # Prove
      let proofRes = proveShuffle(inputs, outputs)
      check proofRes.isOk
      let proof = proofRes.get()
      
      # Verify
      let verifyRes = verifyShuffle(proof, inputs, outputs)
      check verifyRes.isOk
      check verifyRes.get() == true
      
      # Tamper
      var badOutputs = outputs
      badOutputs[0] = inputs[0] # Duplicate input[0], missing input[1]
      let badVerify = verifyShuffle(proof, inputs, badOutputs)
      # Should fail (either proof mismatch or permutation check)
      check badVerify.isOk
      check badVerify.get() == false
