import std/[tables, json, strutils, options]
import chronos
import ./chains/[types, btc_client, evm_client, sol_client, tron_client]

type
  ChainConfig* = object
    chainType*: ChainType
    rpcUrl*: string
    rpcUser*: string
    rpcPassword*: string
    chainId*: int # For EVM

  ChainManager* = ref object
    clients*: Table[ChainType, ChainClient]

proc newChainManager*(configs: seq[ChainConfig]): ChainManager =
  new(result)
  result.clients = initTable[ChainType, ChainClient]()
  
  for cfg in configs:
    try:
      case cfg.chainType
      of ctBtc:
        result.clients[ctBtc] = newBtcClient(cfg.rpcUrl, cfg.rpcUser, cfg.rpcPassword)
      of ctEth:
        result.clients[ctEth] = newEvmClient(ctEth, cfg.rpcUrl, cfg.chainId)
      of ctBsc:
        result.clients[ctBsc] = newEvmClient(ctBsc, cfg.rpcUrl, cfg.chainId)
      of ctSol:
        result.clients[ctSol] = newSolClient(cfg.rpcUrl)
      of ctTron:
        result.clients[ctTron] = newTronClient(cfg.rpcUrl)
    except Exception as e:
      echo "[ChainManager] Failed to init client for ", cfg.chainType, ": ", e.msg

proc getClient*(mgr: ChainManager, chain: ChainType): Option[ChainClient] =
  if mgr.clients.hasKey(chain):
    return some(mgr.clients[chain])
  return none(ChainClient)

proc checkBalance*(mgr: ChainManager, chain: ChainType, address: string): Future[string] {.async.} =
  if mgr.clients.hasKey(chain):
    let client = mgr.clients[chain]
    
    try:
      case chain
      of ctBtc:
        let c = BtcClient(client)
        return await c.getBalance(address)
      of ctEth, ctBsc:
        let c = EvmClient(client)
        return await c.getBalance(address)
      of ctSol:
        let c = SolClient(client)
        return await c.getBalance(address)
      of ctTron:
        let c = TronClient(client)
        return await c.getBalance(address)
    except CatchableError as e:
      echo "[ChainManager] Balance check failed: ", e.msg
      return "0"
  return "0"

