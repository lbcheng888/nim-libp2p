import std/[json, options, os, parseopt, strutils]

import chronos

import ../libp2p/lsmr
import ../libp2p/fabric/node
import ../libp2p/fabric/rpc/server
import ../libp2p/fabric/types
import ../libp2p/multiaddress
import ../libp2p/rwad/codec
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
    legacyBootstrapAddrs: seq[string]
    lsmrBootstrapAddrs: seq[string]
    rpcHost: string
    rpcPort: int
    lsmrPath: seq[uint8]
    routingMode: RoutingPlaneMode
    primaryPlane: PrimaryRoutingPlane
    lsmrNetworkId: string
    lsmrOperatorId: string
    lsmrServeDepth: int
    lsmrMinWitnessQuorum: int
    lsmrAnchors: seq[LsmrAnchor]

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
    legacyBootstrapAddrs: @[],
    lsmrBootstrapAddrs: @[],
    rpcHost: "127.0.0.1",
    rpcPort: 9854,
    lsmrPath: @[5'u8],
    routingMode: RoutingPlaneMode.legacyOnly,
    primaryPlane: PrimaryRoutingPlane.legacy,
    lsmrNetworkId: "",
    lsmrOperatorId: "",
    lsmrServeDepth: 2,
    lsmrMinWitnessQuorum: 1,
    lsmrAnchors: @[],
  )

proc parseRoutingPlaneMode(text: string): RoutingPlaneMode =
  case text.strip().toLowerAscii()
  of "dual", "dualstack", "dual_stack":
    RoutingPlaneMode.dualStack
  of "lsmr", "lsmronly", "lsmr_only":
    RoutingPlaneMode.lsmrOnly
  else:
    RoutingPlaneMode.legacyOnly

proc parsePrimaryRoutingPlane(text: string): PrimaryRoutingPlane =
  case text.strip().toLowerAscii()
  of "lsmr":
    PrimaryRoutingPlane.lsmr
  else:
    PrimaryRoutingPlane.legacy

proc parseBoolText(value: string): bool =
  case value.strip().toLowerAscii()
  of "1", "true", "yes", "on":
    true
  else:
    false

proc parseLsmrPathNode(node: JsonNode): LsmrPath =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    var digit = 0
    case item.kind
    of JInt:
      digit = int(item.getInt())
    of JString:
      try:
        digit = parseInt(item.getStr().strip())
      except ValueError:
        raise newException(ValueError, "invalid lsmr path digit: " & item.getStr())
    else:
      raise newException(ValueError, "invalid lsmr path item kind")
    if digit < 1 or digit > 9:
      raise newException(ValueError, "lsmr path digit out of range")
    result.add(uint8(digit))

proc multiaddrsFromJson(node: JsonNode): seq[MultiAddress] =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    if item.kind != JString:
      continue
    let raw = item.getStr().strip()
    if raw.len == 0:
      continue
    let parsed = MultiAddress.init(raw)
    if parsed.isErr():
      raise newException(ValueError, "invalid multiaddr in lsmr anchor: " & raw)
    result.add(parsed.get())

proc parseLsmrAnchorsJson(value: string): seq[LsmrAnchor] =
  if value.strip().len == 0:
    return @[]
  let node = parseJson(value)
  if node.kind != JArray:
    raise newException(ValueError, "lsmr anchors json must be an array")
  for item in node:
    if item.kind != JObject:
      raise newException(ValueError, "lsmr anchor item must be an object")
    let peerText = item{"peerId"}.getStr(item{"peer_id"}.getStr("")).strip()
    if peerText.len == 0:
      raise newException(ValueError, "lsmr anchor missing peerId")
    let peerId = PeerId.init(peerText).valueOr:
      raise newException(ValueError, "invalid lsmr anchor peerId: " & peerText)
    let regionRaw = max(
      0'i64,
      item{"regionDigit"}.getInt(item{"region_digit"}.getInt(5))
    )
    let serveDepthRaw = max(
      1'i64,
      item{"serveDepth"}.getInt(item{"serve_depth"}.getInt(2))
    )
    let directionMaskRaw = max(
      0'i64,
      item{"directionMask"}.getInt(item{"direction_mask"}.getInt(0x1ff))
    )
    var prefix = parseLsmrPathNode(item{"attestedPrefix"})
    if prefix.len == 0:
      prefix = parseLsmrPathNode(item{"attested_prefix"})
    if prefix.len == 0 and regionRaw > 0:
      prefix = @[uint8(regionRaw)]
    result.add(
      LsmrAnchor(
        peerId: peerId,
        addrs: multiaddrsFromJson(
          if not item{"addrs"}.isNil: item{"addrs"}
          elif not item{"addresses"}.isNil: item{"addresses"}
          else: item{"multiaddrs"}
        ),
        operatorId: item{"operatorId"}.getStr(item{"operator_id"}.getStr("root")),
        regionDigit: uint8(min(255'i64, regionRaw)),
        attestedPrefix: prefix,
        serveDepth: uint8(min(255'i64, serveDepthRaw)),
        directionMask: uint32(directionMaskRaw),
        canIssueRootCert: if item{"canIssueRootCert"}.isNil:
            item{"can_issue_root_cert"}.getBool(true)
          else:
            item{"canIssueRootCert"}.getBool(true),
      )
    )

proc effectiveLsmrBootstrapAddrs(cfg: CliConfig): seq[string] =
  if cfg.lsmrBootstrapAddrs.len > 0:
    return cfg.lsmrBootstrapAddrs
  if cfg.routingMode != RoutingPlaneMode.legacyOnly:
    return cfg.bootstrapAddrs
  @[]

proc effectiveLsmrConfig(cfg: CliConfig, identity: NodeIdentity, genesis: GenesisSpec): Option[LsmrConfig] =
  if cfg.routingMode == RoutingPlaneMode.legacyOnly:
    return none(LsmrConfig)
  let regionDigit =
    if cfg.lsmrPath.len > 0: cfg.lsmrPath[0]
    else: 5'u8
  let localSuffix =
    if cfg.lsmrPath.len > 1: cfg.lsmrPath[1 .. ^1]
    else: @[]
  let networkId =
    if cfg.lsmrNetworkId.strip().len > 0: cfg.lsmrNetworkId.strip()
    else: genesis.params.chainId
  let operatorId =
    if cfg.lsmrOperatorId.strip().len > 0: cfg.lsmrOperatorId.strip()
    else: identity.account
  let anchors =
    if cfg.lsmrAnchors.len > 0:
      cfg.lsmrAnchors
    else:
      @[
        LsmrAnchor(
          peerId: identity.peerId,
          operatorId: operatorId,
          regionDigit: regionDigit,
          attestedPrefix: @[regionDigit],
          serveDepth: uint8(max(1, cfg.lsmrServeDepth)),
          directionMask: 0x1ff'u32,
          canIssueRootCert: true,
        )
      ]
  some(
    LsmrConfig.init(
      networkId = networkId,
      anchors = anchors,
      serveWitness = true,
      operatorId = operatorId,
      regionDigit = regionDigit,
      localSuffix = localSuffix,
      serveDepth = max(1, cfg.lsmrServeDepth),
      minWitnessQuorum = max(1, cfg.lsmrMinWitnessQuorum),
    )
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
      of "legacy-bootstrap-addrs":
        result.legacyBootstrapAddrs = splitCsv(consumeOptionValue(args, index, raw, key))
      of "lsmr-bootstrap-addrs":
        result.lsmrBootstrapAddrs = splitCsv(consumeOptionValue(args, index, raw, key))
      of "rpc-host":
        result.rpcHost = consumeOptionValue(args, index, raw, key)
      of "rpc-port":
        result.rpcPort = parseInt(consumeOptionValue(args, index, raw, key))
      of "lsmr-path":
        result.lsmrPath = @[]
        for item in splitCsv(consumeOptionValue(args, index, raw, key)):
          result.lsmrPath.add(uint8(parseInt(item)))
      of "routing-mode":
        result.routingMode = parseRoutingPlaneMode(consumeOptionValue(args, index, raw, key))
      of "primary-plane":
        result.primaryPlane = parsePrimaryRoutingPlane(consumeOptionValue(args, index, raw, key))
      of "lsmr-network-id":
        result.lsmrNetworkId = consumeOptionValue(args, index, raw, key).strip()
      of "lsmr-operator-id":
        result.lsmrOperatorId = consumeOptionValue(args, index, raw, key).strip()
      of "lsmr-serve-depth":
        result.lsmrServeDepth = parseInt(consumeOptionValue(args, index, raw, key))
      of "lsmr-min-witness-quorum":
        result.lsmrMinWitnessQuorum = parseInt(consumeOptionValue(args, index, raw, key))
      of "lsmr-anchors-json":
        result.lsmrAnchors = parseLsmrAnchorsJson(consumeOptionValue(args, index, raw, key))
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
  let identity = ensureIdentity(cfg.identityPath)
  let genesis = ensureGenesis(cfg.genesisPath, identity, cfg.chainId)
  let lsmrBootstrapAddrs = effectiveLsmrBootstrapAddrs(cfg)
  let lsmrConfig = effectiveLsmrConfig(cfg, identity, genesis)
  let node = newFabricNode(FabricNodeConfig(
    dataDir: cfg.dataDir,
    identityPath: cfg.identityPath,
    genesisPath: cfg.genesisPath,
    listenAddrs: cfg.listenAddrs,
    bootstrapAddrs: cfg.bootstrapAddrs,
    legacyBootstrapAddrs: cfg.legacyBootstrapAddrs,
    lsmrBootstrapAddrs: lsmrBootstrapAddrs,
    lsmrPath: cfg.lsmrPath,
    routingMode: cfg.routingMode,
    primaryPlane: cfg.primaryPlane,
    lsmrConfig: lsmrConfig,
  ))
  let rpc = newFabricRpcServer(node, cfg.rpcHost, cfg.rpcPort)
  proc serve() {.async: (raises: [CancelledError, Exception]).} =
    await node.start()
    await rpc.start()
    let peerId = peerIdString(node.identity.peerId)
    echo $(%{
      "ok": %true,
      "rpc": %("http://" & cfg.rpcHost & ":" & $cfg.rpcPort),
      "mode": %"fabric",
      "peerId": %peerId,
      "account": %node.identity.account,
      "lsmrPath": %cfg.lsmrPath,
      "routingMode": %($cfg.routingMode),
      "primaryPlane": %($cfg.primaryPlane),
      "listenAddrs": %cfg.listenAddrs,
      "bootstrapAddrs": %cfg.bootstrapAddrs,
      "lsmrBootstrapAddrs": %lsmrBootstrapAddrs,
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
