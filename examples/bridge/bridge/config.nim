import std/[json, os, options, parseopt, sequtils, strutils]
import jsonschema
import yaml/tojson as yaml
import bridge/model
import bridge/storage

const
  DefaultTcpListen = "/ip4/0.0.0.0/tcp/0"

type
  SignerTarget* = object
    name*: string
    rpcEndpoint*: string
    hsm*: string

  BridgeAddressMapping* = object
    bsc*: string
    btc*: string

  BridgeConfig* = object
    mode*: BridgeMode
    listenTcp*: seq[string]
    listenQuic*: seq[string]
    peers*: seq[string]
    assets*: seq[string]
    chains*: seq[string]
    intervalMs*: int
    signatureThreshold*: int
    executorIntervalMs*: int
    executorRetryMs*: int
    executorBatchSize*: int
    executorTaskVisibilityMs*: int
    executorTaskMaxAttempts*: int
    storageBackend*: StorageBackend
    storagePath*: string
    signerTargets*: seq[SignerTarget]
    bscRpcUrl*: string
    bscUsdcContract*: string
    bscVaultAddress*: string
    bscStartBlock*: int64
    bscPollIntervalMs*: int
    minBridgeAmount*: float
    btcRpcUrl*: string
    btcRpcUser*: string
    btcRpcPassword*: string
    btcUsdPrice*: float
    defaultBtcAddress*: string
    addressMappings*: seq[BridgeAddressMapping]
    enableOnionDemo*: bool
    onionDemoHops*: int
    rawConfig*: JsonNode

proc parseMode(value: string): BridgeMode =
  case value.toLowerAscii()
  of "watcher": bmWatcher
  of "signer": bmSigner
  else: bmExecutor

proc parseStorage(value: string): StorageBackend =
  case value.toLowerAscii()
  of "file", "fs": sbFile
  of "pebble", "pebblekv", "kv": sbPebble
  else: sbInMemory

proc defaultConfig*(): BridgeConfig =
  BridgeConfig(
    mode: bmExecutor,
    listenTcp: @[DefaultTcpListen],
    listenQuic: @[],
    peers: @[],
    assets: @["ETH", "USDC"],
    chains: @["chainA", "chainB", "chainC"],
    intervalMs: 5000,
    signatureThreshold: 1,
    executorIntervalMs: 2000,
    executorRetryMs: 2000,
    executorBatchSize: 16,
    executorTaskVisibilityMs: DefaultVisibilityMs,
    executorTaskMaxAttempts: DefaultMaxAttempts,
    storageBackend: sbInMemory,
    storagePath: "bridge-data",
    signerTargets: @[],
    bscRpcUrl: "",
    bscUsdcContract: "",
    bscVaultAddress: "",
    bscStartBlock: 0,
    bscPollIntervalMs: 6000,
    minBridgeAmount: 0.0,
    btcRpcUrl: "",
    btcRpcUser: "",
    btcRpcPassword: "",
    btcUsdPrice: 68000.0,
    defaultBtcAddress: "",
    addressMappings: @[],
    enableOnionDemo: false,
    onionDemoHops: 3,
    rawConfig: newJNull()
  )

proc loadJsonNode(path: string): Option[JsonNode] =
  if path.len == 0 or not fileExists(path):
    return none(JsonNode)
  try:
    some(parseJson(readFile(path)))
  except CatchableError:
    none(JsonNode)

proc loadYamlNode(path: string): Option[JsonNode] =
  if path.len == 0 or not fileExists(path):
    return none(JsonNode)
  try:
    let docs = yaml.loadToJson(readFile(path))
    if docs.len == 0:
      return none(JsonNode)
    some(docs[0])
  except CatchableError:
    none(JsonNode)

jsonSchema:
  SignerSchema:
    name?: string
    rpcEndpoint?: string
    hsm?: string

  AddressMappingSchema:
    bsc?: string
    btc?: string

  BridgeSchema:
    mode?: string
    listen?: string[]
    listenQuic?: string[]
    peers?: string[]
    assets?: string[]
    chains?: string[]
    intervalMs?: int
    signatureThreshold?: int
    executorIntervalMs?: int
    executorRetryMs?: int
    executorBatchSize?: int
    executorTaskVisibilityMs?: int
    executorTaskMaxAttempts?: int
    storageBackend?: string
    storagePath?: string
    signers?: SignerSchema[]
    bscRpcUrl?: string
    bscUsdcContract?: string
    bscVaultAddress?: string
    bscStartBlock?: int
    bscPollIntervalMs?: int
    minBridgeAmount?: float
    btcRpcUrl?: string
    btcRpcUser?: string
    btcRpcPassword?: string
    btcUsdPrice?: float
    defaultBtcAddress?: string
    addressMappings?: AddressMappingSchema[]
    enableOnionDemo?: bool
    onionDemoHops?: int

proc applyNode(cfg: var BridgeConfig; node: JsonNode) =
  if node.isNil() or node.kind != JObject:
    return
  cfg.rawConfig = node
  if node.hasKey("mode"):
    cfg.mode = parseMode(node["mode"].getStr())
  if node.hasKey("listen") and node["listen"].kind == JArray:
    cfg.listenTcp = node["listen"].elems.mapIt(it.getStr())
  if node.hasKey("listenQuic") and node["listenQuic"].kind == JArray:
    cfg.listenQuic = node["listenQuic"].elems.mapIt(it.getStr())
  if node.hasKey("peers") and node["peers"].kind == JArray:
    cfg.peers = node["peers"].elems.mapIt(it.getStr())
  if node.hasKey("assets") and node["assets"].kind == JArray:
    cfg.assets = node["assets"].elems.mapIt(it.getStr())
  if node.hasKey("chains") and node["chains"].kind == JArray:
    cfg.chains = node["chains"].elems.mapIt(it.getStr())
  if node.hasKey("intervalMs"):
    cfg.intervalMs = max(node["intervalMs"].getInt(), 100)
  if node.hasKey("signatureThreshold"):
    cfg.signatureThreshold = max(node["signatureThreshold"].getInt(), 1)
  if node.hasKey("executorIntervalMs"):
    cfg.executorIntervalMs = max(node["executorIntervalMs"].getInt(), 250)
  if node.hasKey("executorRetryMs"):
    cfg.executorRetryMs = max(node["executorRetryMs"].getInt(), 250)
  if node.hasKey("executorBatchSize"):
    cfg.executorBatchSize = max(node["executorBatchSize"].getInt(), 1)
  if node.hasKey("executorTaskVisibilityMs"):
    cfg.executorTaskVisibilityMs = max(node["executorTaskVisibilityMs"].getInt(), 100)
  if node.hasKey("executorTaskMaxAttempts"):
    cfg.executorTaskMaxAttempts = max(node["executorTaskMaxAttempts"].getInt(), 1)
  if node.hasKey("storageBackend"):
    cfg.storageBackend = parseStorage(node["storageBackend"].getStr())
  if node.hasKey("storagePath"):
    cfg.storagePath = node["storagePath"].getStr()
  if node.hasKey("signers") and node["signers"].kind == JArray:
    cfg.signerTargets.setLen(0)
    for signerNode in node["signers"].elems:
      if signerNode.kind != JObject:
        continue
      let name =
        if signerNode.hasKey("name"): signerNode["name"].getStr() else: ""
      let endpoint =
        if signerNode.hasKey("rpcEndpoint"): signerNode["rpcEndpoint"].getStr() else: ""
      let hsmPath =
        if signerNode.hasKey("hsm"): signerNode["hsm"].getStr() else: ""
      cfg.signerTargets.add(SignerTarget(name: name, rpcEndpoint: endpoint, hsm: hsmPath))
  if node.hasKey("bscRpcUrl"):
    cfg.bscRpcUrl = node["bscRpcUrl"].getStr()
  if node.hasKey("bscUsdcContract"):
    cfg.bscUsdcContract = node["bscUsdcContract"].getStr()
  if node.hasKey("bscVaultAddress"):
    cfg.bscVaultAddress = node["bscVaultAddress"].getStr()
  if node.hasKey("bscStartBlock"):
    cfg.bscStartBlock = node["bscStartBlock"].getInt().int64
  if node.hasKey("bscPollIntervalMs"):
    cfg.bscPollIntervalMs = max(node["bscPollIntervalMs"].getInt(), 1000)
  if node.hasKey("minBridgeAmount"):
    cfg.minBridgeAmount = max(node["minBridgeAmount"].getFloat(), 0.0)
  if node.hasKey("btcRpcUrl"):
    cfg.btcRpcUrl = node["btcRpcUrl"].getStr()
  if node.hasKey("btcRpcUser"):
    cfg.btcRpcUser = node["btcRpcUser"].getStr()
  if node.hasKey("btcRpcPassword"):
    cfg.btcRpcPassword = node["btcRpcPassword"].getStr()
  if node.hasKey("btcUsdPrice"):
    cfg.btcUsdPrice = max(node["btcUsdPrice"].getFloat(), 1.0)
  if node.hasKey("defaultBtcAddress"):
    cfg.defaultBtcAddress = node["defaultBtcAddress"].getStr()
  if node.hasKey("addressMappings") and node["addressMappings"].kind == JArray:
    cfg.addressMappings.setLen(0)
    for entry in node["addressMappings"]:
      if entry.kind != JObject:
        continue
      let bsc =
        if entry.hasKey("bsc"): entry["bsc"].getStr() else: ""
      let btc =
        if entry.hasKey("btc"): entry["btc"].getStr() else: ""
      if bsc.len > 0 and btc.len > 0:
        cfg.addressMappings.add(BridgeAddressMapping(bsc: bsc, btc: btc))
  if node.hasKey("enableOnionDemo"):
    cfg.enableOnionDemo = node["enableOnionDemo"].getBool()
  if node.hasKey("onionDemoHops"):
    cfg.onionDemoHops = max(2, node["onionDemoHops"].getInt())

proc loadConfigFromFile(path: string): Option[JsonNode] =
  let node =
    if path.toLowerAscii().endsWith(".yaml") or path.toLowerAscii().endsWith(".yml"):
      loadYamlNode(path)
    else:
      loadJsonNode(path)
  if node.isNone():
    return none(JsonNode)
  if not node.get().isValid(BridgeSchema):
    echo "[config] schema validation failed: ", path
    return none(JsonNode)
  node

proc applyEnvOverrides(cfg: var BridgeConfig) =
  let modeEnv = getEnv("BRIDGE_MODE")
  if modeEnv.len > 0:
    cfg.mode = parseMode(modeEnv)
  let listenEnv = getEnv("BRIDGE_LISTEN")
  if listenEnv.len > 0:
    cfg.listenTcp = listenEnv.split(',').mapIt(it.strip()).filterIt(it.len > 0)
  let listenQuicEnv = getEnv("BRIDGE_LISTEN_QUIC")
  if listenQuicEnv.len > 0:
    cfg.listenQuic = listenQuicEnv.split(',').mapIt(it.strip()).filterIt(it.len > 0)
  let peersEnv = getEnv("BRIDGE_PEERS")
  if peersEnv.len > 0:
    cfg.peers = peersEnv.split(',').mapIt(it.strip()).filterIt(it.len > 0)
  let assetsEnv = getEnv("BRIDGE_ASSETS")
  if assetsEnv.len > 0:
    cfg.assets = assetsEnv.split(',').mapIt(it.strip()).filterIt(it.len > 0)
  let chainsEnv = getEnv("BRIDGE_CHAINS")
  if chainsEnv.len > 0:
    cfg.chains = chainsEnv.split(',').mapIt(it.strip()).filterIt(it.len > 0)
  let intervalEnv = getEnv("BRIDGE_INTERVAL_MS")
  if intervalEnv.len > 0:
    try:
      cfg.intervalMs = max(parseInt(intervalEnv), 100)
    except ValueError:
      discard
  let thresholdEnv = getEnv("BRIDGE_SIG_THRESHOLD")
  if thresholdEnv.len > 0:
    try:
      cfg.signatureThreshold = max(parseInt(thresholdEnv), 1)
    except ValueError:
      discard
  let execIntervalEnv = getEnv("BRIDGE_EXECUTOR_INTERVAL_MS")
  if execIntervalEnv.len > 0:
    try:
      cfg.executorIntervalMs = max(parseInt(execIntervalEnv), 250)
    except ValueError:
      discard
  let execRetryEnv = getEnv("BRIDGE_EXECUTOR_RETRY_MS")
  if execRetryEnv.len > 0:
    try:
      cfg.executorRetryMs = max(parseInt(execRetryEnv), 250)
    except ValueError:
      discard
  let execBatchEnv = getEnv("BRIDGE_EXECUTOR_BATCH")
  if execBatchEnv.len > 0:
    try:
      cfg.executorBatchSize = max(parseInt(execBatchEnv), 1)
    except ValueError:
      discard
  let taskVisibilityEnv = getEnv("BRIDGE_TASK_VISIBILITY_MS")
  if taskVisibilityEnv.len > 0:
    try:
      cfg.executorTaskVisibilityMs = max(parseInt(taskVisibilityEnv), 100)
    except ValueError:
      discard
  let taskAttemptsEnv = getEnv("BRIDGE_TASK_MAX_ATTEMPTS")
  if taskAttemptsEnv.len > 0:
    try:
      cfg.executorTaskMaxAttempts = max(parseInt(taskAttemptsEnv), 1)
    except ValueError:
      discard
  let backendEnv = getEnv("BRIDGE_STORAGE_BACKEND")
  if backendEnv.len > 0:
    cfg.storageBackend = parseStorage(backendEnv)
  let pathEnv = getEnv("BRIDGE_STORAGE_PATH")
  if pathEnv.len > 0:
    cfg.storagePath = pathEnv
  let bscRpcEnv = getEnv("BRIDGE_BSC_RPC_URL")
  if bscRpcEnv.len > 0:
    cfg.bscRpcUrl = bscRpcEnv
  let bscUsdcEnv = getEnv("BRIDGE_BSC_USDC_CONTRACT")
  if bscUsdcEnv.len > 0:
    cfg.bscUsdcContract = bscUsdcEnv
  let onionDemoEnv = getEnv("BRIDGE_ONION_DEMO")
  if onionDemoEnv.len > 0:
    cfg.enableOnionDemo = onionDemoEnv.toLowerAscii() in ["1", "true", "yes"]
  let onionHopsEnv = getEnv("BRIDGE_ONION_HOPS")
  if onionHopsEnv.len > 0:
    try:
      cfg.onionDemoHops = max(2, parseInt(onionHopsEnv))
    except ValueError:
      discard
  let bscVaultEnv = getEnv("BRIDGE_BSC_VAULT")
  if bscVaultEnv.len > 0:
    cfg.bscVaultAddress = bscVaultEnv
  let bscStartEnv = getEnv("BRIDGE_BSC_START_BLOCK")
  if bscStartEnv.len > 0:
    try:
      cfg.bscStartBlock = parseInt(bscStartEnv).int64
    except ValueError:
      discard
  let bscPollEnv = getEnv("BRIDGE_BSC_POLL_MS")
  if bscPollEnv.len > 0:
    try:
      cfg.bscPollIntervalMs = max(parseInt(bscPollEnv), 1000)
    except ValueError:
      discard
  let minAmountEnv = getEnv("BRIDGE_MIN_USDC")
  if minAmountEnv.len > 0:
    try:
      cfg.minBridgeAmount = max(parseFloat(minAmountEnv), 0.0)
    except ValueError:
      discard
  let btcRpcEnv = getEnv("BRIDGE_BTC_RPC_URL")
  if btcRpcEnv.len > 0:
    cfg.btcRpcUrl = btcRpcEnv
  let btcUserEnv = getEnv("BRIDGE_BTC_RPC_USER")
  if btcUserEnv.len > 0:
    cfg.btcRpcUser = btcUserEnv
  let btcPassEnv = getEnv("BRIDGE_BTC_RPC_PASSWORD")
  if btcPassEnv.len > 0:
    cfg.btcRpcPassword = btcPassEnv
  let btcPriceEnv = getEnv("BRIDGE_BTC_USD_PRICE")
  if btcPriceEnv.len > 0:
    try:
      cfg.btcUsdPrice = max(parseFloat(btcPriceEnv), 1.0)
    except ValueError:
      discard
  let btcDefaultEnv = getEnv("BRIDGE_BTC_DEFAULT_ADDRESS")
  if btcDefaultEnv.len > 0:
    cfg.defaultBtcAddress = btcDefaultEnv
  let mapEnv = getEnv("BRIDGE_ADDRESS_MAPS")
  if mapEnv.len > 0:
    cfg.addressMappings.setLen(0)
    for entry in mapEnv.split({';', ','}):
      let parts = entry.split('=')
      if parts.len == 2 and parts[0].len > 0 and parts[1].len > 0:
        cfg.addressMappings.add(BridgeAddressMapping(bsc: parts[0], btc: parts[1]))

proc parseCli*(): BridgeConfig =
  var cfg = defaultConfig()
  var customListen = false
  var parser = initOptParser()
  var configLoaded = false
  for kind, key, value in parser.getopt():
    case kind
    of cmdArgument:
      cfg.peers.add(key)
    of cmdShortOption, cmdLongOption:
      case key
      of "config":
        if value.len > 0 and not configLoaded:
          let node = loadConfigFromFile(value)
          if node.isSome():
            cfg.applyNode(node.get())
            configLoaded = true
      of "mode":
        cfg.mode = parseMode(value)
      of "listen":
        if not customListen:
          cfg.listenTcp.setLen(0)
          customListen = true
        if value.len > 0:
          cfg.listenTcp.add(value)
      of "listen-quic":
        if value.len > 0:
          cfg.listenQuic.add(value)
      of "peer":
        if value.len > 0:
          cfg.peers.add(value)
      of "asset":
        if value.len > 0:
          cfg.assets.add(value)
      of "chain":
        if value.len > 0:
          cfg.chains.add(value)
      of "interval":
        try:
          cfg.intervalMs = max(parseInt(value), 100)
        except ValueError:
          discard
      of "storage-backend":
        cfg.storageBackend = parseStorage(value)
      of "storage-path":
        if value.len > 0:
          cfg.storagePath = value
      of "sig-threshold":
        try:
          cfg.signatureThreshold = max(parseInt(value), 1)
        except ValueError:
          discard
      of "executor-interval":
        try:
          cfg.executorIntervalMs = max(parseInt(value), 250)
        except ValueError:
          discard
      of "executor-retry":
        try:
          cfg.executorRetryMs = max(parseInt(value), 250)
        except ValueError:
          discard
      of "executor-batch":
        try:
          cfg.executorBatchSize = max(parseInt(value), 1)
        except ValueError:
          discard
      of "task-visibility":
        try:
          cfg.executorTaskVisibilityMs = max(parseInt(value), 100)
        except ValueError:
          discard
      of "task-max-attempts":
        try:
          cfg.executorTaskMaxAttempts = max(parseInt(value), 1)
        except ValueError:
          discard
      of "bsc-rpc":
        if value.len > 0:
          cfg.bscRpcUrl = value
      of "bsc-usdc":
        if value.len > 0:
          cfg.bscUsdcContract = value
      of "bsc-vault":
        if value.len > 0:
          cfg.bscVaultAddress = value
      of "bsc-start-block":
        try:
          cfg.bscStartBlock = parseInt(value).int64
        except ValueError:
          discard
      of "bsc-poll":
        try:
          cfg.bscPollIntervalMs = max(parseInt(value), 1000)
        except ValueError:
          discard
      of "min-usdc":
        try:
          cfg.minBridgeAmount = max(parseFloat(value), 0.0)
        except ValueError:
          discard
      of "btc-rpc":
        if value.len > 0:
          cfg.btcRpcUrl = value
      of "btc-user":
        cfg.btcRpcUser = value
      of "btc-password":
        cfg.btcRpcPassword = value
      of "btc-price":
        try:
          cfg.btcUsdPrice = max(parseFloat(value), 1.0)
        except ValueError:
          discard
      of "btc-default-address":
        cfg.defaultBtcAddress = value
      of "enable-onion-demo":
        cfg.enableOnionDemo = true
      of "onion-hops":
        try:
          cfg.onionDemoHops = max(parseInt(value), 2)
        except ValueError:
          discard
      of "address-map":
        if value.len > 0:
          let parts = value.split('=')
          if parts.len == 2 and parts[0].len > 0 and parts[1].len > 0:
            cfg.addressMappings.add(BridgeAddressMapping(bsc: parts[0], btc: parts[1]))
      else:
        discard
    of cmdEnd:
      discard
  cfg.assets = cfg.assets.filterIt(it.len > 0)
  if cfg.assets.len == 0:
    cfg.assets = @["ETH"]
  cfg.chains = cfg.chains.filterIt(it.len > 0)
  if cfg.chains.len < 2:
    cfg.chains = @["chainA", "chainB"]
  cfg.applyEnvOverrides()
  cfg
