# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import results

import ../secp
import ./types
import ./secp_utils

export encodeAmount, hkdfBlind

proc combineAmountAndBlind(amount: CoinJoinAmount, blind: CoinJoinBlind): CoinJoinResult[SkPublicKey] =
  let amountPub = ?generatorMul(amount)
  let blindPub = ?pedersenGeneratorMul(blind)
  combineKeys([amountPub, blindPub])

proc makeCommitment(pub: SkPublicKey, blind: CoinJoinBlind): CoinJoinCommitment =
  CoinJoinCommitment(point: serializePoint(pub), blind: blind)

proc pedersenCommit*(
    amount: CoinJoinAmount, blind: CoinJoinBlind
): CoinJoinResult[CoinJoinCommitment] =
  if not coinJoinSupportEnabled():
    return errDisabled[CoinJoinCommitment]("pedersenCommit")
  let combined = ?combineAmountAndBlind(amount, blind)
  ok(makeCommitment(combined, blind))

proc verifyCommitment*(
    commitment: CoinJoinCommitment, amount: CoinJoinAmount, blind: CoinJoinBlind
): CoinJoinResult[bool] =
  if not coinJoinSupportEnabled():
    return errDisabled[bool]("verifyCommitment")
  let expected = ?combineAmountAndBlind(amount, blind)
  let stored = ?commitmentPub(commitment)
  ok(stored == expected)

proc addCommitments*(
    left, right: CoinJoinCommitment
): CoinJoinResult[CoinJoinCommitment] =
  if not coinJoinSupportEnabled():
    return errDisabled[CoinJoinCommitment]("addCommitments")
  let leftPub = ?commitmentPub(left)
  let rightPub = ?commitmentPub(right)
  let sumPub = ?combineKeys([leftPub, rightPub])
  let blind = ?addBlinds(left.blind, right.blind)
  ok(makeCommitment(sumPub, blind))

proc subtractCommitments*(
    left, right: CoinJoinCommitment
): CoinJoinResult[CoinJoinCommitment] =
  if not coinJoinSupportEnabled():
    return errDisabled[CoinJoinCommitment]("subtractCommitments")
  let leftPub = ?commitmentPub(left)
  let rightPub = ?commitmentPub(right)
  let negRight = ?negateKey(rightPub)
  let resultPub = ?combineKeys([leftPub, negRight])
  let blind = ?subBlinds(left.blind, right.blind)
  ok(makeCommitment(resultPub, blind))
