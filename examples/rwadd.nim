import std/[json, os, parseopt, strutils]

import chronos

import ../libp2p/fabric
import ../libp2p/rwad/identity
import ../libp2p/rwad/types

type
  CliConfig = object
    command: string
    dataDir: string
    identityPath: string
    genesisPath: string
    outPath: string
    chainId: string
    listenAddrs: seq[string]
    bootstrapAddrs: seq[string]
    rpcHost: string
    rpcPort: int
    lsmrPath: seq[uint8]

proc splitCsv(value: string): seq[string] =
  for item in value.split(','):
    let trimmed = item.strip()
    if trimmed.len > 0:
      result.add(trimmed)

proc defaults(): CliConfig =
  CliConfig(
    command: "",
    dataDir: getCurrentDir() / ".rwad",
    identityPath: "",
    genesisPath: "",
    outPath: "",
    chainId: "rwad-local",
    listenAddrs: @["/ip4/127.0.0.1/tcp/4001"],
    bootstrapAddrs: @[],
    rpcHost: "127.0.0.1",
    rpcPort: 9854,
    lsmrPath: @[5'u8],
  )

proc consumeOptionValue(args: seq[string], index: var int, raw: string, key: string): string =
  let eqPos = raw.find('=')
  if eqPos >= 0:
    result = raw[eqPos + 1 .. ^1]
  else:
    if index + 1 >= args.len:
      raise newException(ValueError, "missing value for --" & key)
    inc index
    result = args[index]

proc parseCli(): CliConfig =
  result = defaults()
  let args = commandLineParams()
  if args.len == 0:
    return
  result.command = args[0]
  var index = 1
  while index < args.len:
    let raw = args[index]
    if raw.len == 0:
      inc index
      continue
    if raw.startsWith("--"):
      let keyEnd = raw.find('=')
      let key =
        if keyEnd >= 0:
          raw[2 ..< keyEnd]
        else:
          raw[2 .. ^1]
      case key
      of "data-dir":
        result.dataDir = consumeOptionValue(args, index, raw, key)
      of "identity":
        result.identityPath = consumeOptionValue(args, index, raw, key)
      of "genesis":
        result.genesisPath = consumeOptionValue(args, index, raw, key)
      of "out":
        result.outPath = consumeOptionValue(args, index, raw, key)
      of "chain-id":
        result.chainId = consumeOptionValue(args, index, raw, key)
      of "listen-addrs":
        result.listenAddrs = splitCsv(consumeOptionValue(args, index, raw, key))
      of "bootstrap-addrs":
        result.bootstrapAddrs = splitCsv(consumeOptionValue(args, index, raw, key))
      of "rpc-host":
        result.rpcHost = consumeOptionValue(args, index, raw, key)
      of "rpc-port":
        result.rpcPort = parseInt(consumeOptionValue(args, index, raw, key))
      of "lsmr-path":
        result.lsmrPath = @[]
        for item in splitCsv(consumeOptionValue(args, index, raw, key)):
          result.lsmrPath.add(uint8(parseInt(item)))
      else:
        raise newException(ValueError, "unknown option: --" & key)
    elif raw.startsWith("-"):
      raise newException(ValueError, "short options are not supported: " & raw)
    inc index
  if result.identityPath.len == 0:
    result.identityPath = defaultIdentityPath(result.dataDir)
  if result.genesisPath.len == 0:
    result.genesisPath = defaultGenesisPath(result.dataDir)

proc ensureIdentity(path: string): NodeIdentity =
  if fileExists(path):
    return loadIdentity(path)
  let identity = newNodeIdentity()
  saveIdentity(path, identity)
  identity

proc ensureGenesis(path: string, identity: NodeIdentity, chainId: string): GenesisSpec =
  if fileExists(path):
    return loadGenesis(path)
  let genesis = createDefaultGenesis(identity, chainId)
  saveGenesis(path, genesis)
  genesis

proc cmdKeygen(cfg: CliConfig) =
  let identity = newNodeIdentity()
  if cfg.outPath.len > 0:
    saveIdentity(cfg.outPath, identity)
  echo $encodeIdentityJson(identity)

proc cmdGenesis(cfg: CliConfig) =
  let identity = ensureIdentity(cfg.identityPath)
  let genesis = createDefaultGenesis(identity, cfg.chainId)
  let outPath = if cfg.outPath.len > 0: cfg.outPath else: cfg.genesisPath
  saveGenesis(outPath, genesis)
  echo encodeObj(genesis)

proc cmdInit(cfg: CliConfig) =
  createDir(cfg.dataDir)
  let identity = ensureIdentity(cfg.identityPath)
  let genesis = ensureGenesis(cfg.genesisPath, identity, cfg.chainId)
  let peerId = peerIdString(identity.peerId)
  echo $(%{
    "ok": %true,
    "dataDir": %cfg.dataDir,
    "identityPath": %cfg.identityPath,
    "genesisPath": %cfg.genesisPath,
    "account": %identity.account,
    "peerId": %peerId,
    "chainId": %genesis.params.chainId,
  })

proc cmdRun(cfg: CliConfig) =
  discard ensureGenesis(cfg.genesisPath, ensureIdentity(cfg.identityPath), cfg.chainId)
  let node = newFabricNode(FabricNodeConfig(
    dataDir: cfg.dataDir,
    identityPath: cfg.identityPath,
    genesisPath: cfg.genesisPath,
    lsmrPath: cfg.lsmrPath,
  ))
  let rpc = newFabricRpcServer(node, cfg.rpcHost, cfg.rpcPort)
  proc serve() {.async: (raises: [CancelledError, Exception]).} =
    await rpc.start()
    let peerId = peerIdString(node.identity.peerId)
    echo $(%{
      "ok": %true,
      "rpc": %("http://" & cfg.rpcHost & ":" & $cfg.rpcPort),
      "mode": %"fabric",
      "peerId": %peerId,
      "account": %node.identity.account,
      "lsmrPath": %cfg.lsmrPath,
      "listenAddrs": %cfg.listenAddrs,
      "bootstrapAddrs": %cfg.bootstrapAddrs,
    })
    while true:
      await sleepAsync(1.hours)
  waitFor serve()

proc main() =
  let cfg = parseCli()
  case cfg.command
  of "keygen":
    cmdKeygen(cfg)
  of "genesis":
    cmdGenesis(cfg)
  of "init":
    cmdInit(cfg)
  of "run":
    cmdRun(cfg)
  else:
    echo "usage: rwadd <keygen|genesis|init|run> [options]"

main()
