## Base Chain Interface for Multi-Chain Support
## Defines the contract that any blockchain client must implement.

import std/[json, options, strutils]
import chronicles
import ./types

type
  ChainClient* = ref object of RootObj
    rpcUrl*: string
    chainId*: ChainId

  # Common error types
  ChainError* = object of CatchableError
  NetworkError* = object of ChainError
  TransactionError* = object of ChainError

method getBalance*(self: ChainClient, address: string, asset: AssetId): Future[float] {.base, async.} =
  raise newException(ChainError, "Method getBalance not implemented")

method sendTransaction*(self: ChainClient, signedTx: string): Future[string] {.base, async.} =
  ## Broadcasts a signed transaction and returns the transaction hash
  raise newException(ChainError, "Method sendTransaction not implemented")

method getTransactionStatus*(self: ChainClient, txHash: string): Future[SettlementStatus] {.base, async.} =
  raise newException(ChainError, "Method getTransactionStatus not implemented")

method estimateGas*(self: ChainClient, fromAddr, toAddr: string, amount: float, data: string = ""): Future[float] {.base, async.} =
  raise newException(ChainError, "Method estimateGas not implemented")

# Helper to format amounts based on decimals
proc formatAmount*(amount: float, decimals: int): string =
  # Implement robust float -> string conversion avoiding scientific notation
  # and handling precision correctly.
  # For MVP, simple string conversion:
  var s = formatFloat(amount, ffDecimal, decimals)
  # Remove trailing zeros
  if s.contains('.'):
    s = s.strip(chars = {'0'}, trailing = true)
    if s.endsWith('.'): s = s[0..^2]
  return s

