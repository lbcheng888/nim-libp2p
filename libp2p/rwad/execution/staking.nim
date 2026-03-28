import std/[algorithm, math, strutils, tables]

import ./state
import ../types

proc validatorVotingPower*(validator: ValidatorInfo): uint64 =
  validator.stake + validator.delegatedStake

proc totalVotingPower*(state: ChainState): uint64 =
  for _, validator in state.validators.pairs:
    if validator.active and not validator.jailed:
      result += validator.validatorVotingPower()

proc quorumWeight*(state: ChainState): uint64 =
  let total = state.totalVotingPower()
  if total == 0:
    return 0
  let numerator = total * state.params.quorumNumerator
  let denominator = state.params.quorumDenominator
  (numerator + denominator - 1) div denominator

proc activeValidators*(state: ChainState): seq[ValidatorInfo] =
  for _, validator in state.validators.pairs:
    if validator.active and not validator.jailed:
      result.add(validator)
  result.sort(proc(a, b: ValidatorInfo): int = cmp(a.address, b.address))

proc leaderAddressForRound*(state: ChainState, round: uint64): string =
  let validators = state.activeValidators()
  if validators.len == 0:
    return ""
  validators[int(round mod validators.len.uint64)].address

proc applyValidatorRegister*(
    state: ChainState,
    address: string,
    peerId: string,
    publicKey: string,
    stake: uint64,
    at: int64,
) =
  let current = state.validators.getOrDefault(
    address,
    ValidatorInfo(
      address: address,
      peerId: peerId,
      publicKey: publicKey,
      stake: 0,
      delegatedStake: 0,
      active: true,
      jailed: false,
      slashedAmount: 0,
      lastUpdatedAt: at,
    ),
  )
  var validator = current
  validator.peerId = peerId
  validator.publicKey = publicKey
  validator.stake = max(validator.stake, stake)
  validator.active = true
  validator.lastUpdatedAt = at
  state.validators[address] = validator

proc applyStake*(state: ChainState, sender: string, validatorAddress: string, amount: uint64, at: int64) =
  if validatorAddress notin state.validators:
    raise newException(ValueError, "unknown validator: " & validatorAddress)
  if state.rwadBalanceOf(sender) < amount:
    raise newException(ValueError, "insufficient balance for stake")
  var validator = state.validators[validatorAddress]
  validator.stake += amount
  validator.lastUpdatedAt = at
  state.validators[validatorAddress] = validator
  state.adjustRwadBalance(sender, -int64(amount))

proc applyDelegate*(state: ChainState, sender: string, validatorAddress: string, amount: uint64, at: int64) =
  if validatorAddress notin state.validators:
    raise newException(ValueError, "unknown validator: " & validatorAddress)
  if state.rwadBalanceOf(sender) < amount:
    raise newException(ValueError, "insufficient balance for delegation")
  let key = sender & "->" & validatorAddress
  var delegation = state.delegations.getOrDefault(
    key,
    Delegation(delegator: sender, validator: validatorAddress, amount: 0, updatedAt: at),
  )
  delegation.amount += amount
  delegation.updatedAt = at
  state.delegations[key] = delegation
  var validator = state.validators[validatorAddress]
  validator.delegatedStake += amount
  validator.lastUpdatedAt = at
  state.validators[validatorAddress] = validator
  state.adjustRwadBalance(sender, -int64(amount))

proc parseSlashingReason*(value: string): SlashingReason =
  case value.toLowerAscii()
  of "double_sign":
    srDoubleSign
  of "offline":
    srOffline
  else:
    srInvalidCertificate

proc applySlash*(
    state: ChainState,
    offender: string,
    reason: SlashingReason,
    amount: uint64,
    at: int64,
) =
  if offender notin state.validators:
    raise newException(ValueError, "unknown validator: " & offender)
  var validator = state.validators[offender]
  let slashAmount = min(amount, validator.stake)
  validator.stake -= slashAmount
  validator.slashedAmount += slashAmount
  validator.lastUpdatedAt = at
  if validator.stake == 0:
    validator.active = false
    validator.jailed = true
  state.validators[offender] = validator
  state.adjustRwadBalance(state.params.rewardPoolAccount, int64(slashAmount))
  discard reason

proc applyParamUpdate*(state: ChainState, params: ChainParams) =
  state.params = params
