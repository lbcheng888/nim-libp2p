import std/[os, strformat, times]
import chronos
import libp2p
import libp2p/protocols/pubsub/gossipsub

import ./dex_node
import ./types
import ./marketdata/kline_store
import ./storage/order_store
import ./security/signing

proc nowMillis(): int64 =
  int64(epochTime() * 1000.0)

proc buildDex(name: string, mode: DexMode): Future[(Switch, GossipSub, DexNode)] {.async: (raises: [CatchableError]).} =
  let rng = newRng()
  let sw = newStandardSwitch(rng = rng)
  let params = GossipSubParams.init(floodPublish = true)
  var gossip = GossipSub.init(
    switch = sw,
    parameters = params,
    verifySignature = true,
    sign = true
  )
  gossip.PubSub.triggerSelf = true
  sw.mount(gossip.PubSub)
  await sw.start()

  let dir = getTempDir() / &"nim_libp2p_dex_demo_{name}_{nowMillis()}"
  let store =
    try:
      initOrderStore(dir)
    except CatchableError as exc:
      raise exc
    except Exception as exc:
      raise newException(CatchableError, exc.msg)
  let signingCfg = SigningConfig(
    mode: smDisabled,
    keyPath: dir / DefaultKeyFile,
    requireRemoteSignature: false,
    allowUnsigned: true
  )
  let signingCtx = initSigning(signingCfg)
  let cfg = CliConfig(
    mode: mode,
    dataDir: dir,
    metricsFile: "",
    metricsInterval: 10,
    enableMixer: false,
    enableAutoMatch: false,
    enableAutoTrade: false
  )
  let dex = await initDexNode(sw, gossip, cfg, store, signingCtx)
  result = (sw, gossip, dex)

proc main() {.async: (raises: [CatchableError]).} =
  let (matcherSw, _, matcherDex) = await buildDex("matcher", dmMatcher)
  let (traderSw, _, traderDex) = await buildDex("trader", dmTrader)

  matcherDex.onEvent = proc(topic: string, payload: string) {.gcsafe, raises: [].} =
    echo "[matcher] ", topic, " ", payload
  traderDex.onEvent = proc(topic: string, payload: string) {.gcsafe, raises: [].} =
    echo "[trader] ", topic, " ", payload

  handleOrders(matcherDex)
  handleMatches(matcherDex)
  subscribeTrades(matcherDex)

  handleOrders(traderDex)
  handleMatches(traderDex)
  subscribeTrades(traderDex)

  await traderSw.connect(matcherSw.peerInfo.peerId, matcherSw.peerInfo.addrs)
  # Allow pubsub to finish handshake + exchange subscriptions before publish.
  await sleepAsync(800)

  let order = OrderMessage(
    id: "demo-" & $nowMillis(),
    traderPeer: $traderSw.peerInfo.peerId,
    baseAsset: AssetDef(chainId: ChainBTC, symbol: "BTC", decimals: 8, assetType: AssetNative),
    quoteAsset: AssetDef(chainId: ChainBSC, symbol: "USDC", decimals: 6, assetType: AssetERC20),
    side: "buy",
    price: 65_000.0,
    amount: 0.01,
    ttlMs: 60_000,
    timestamp: nowMillis(),
    signature: "",
    signerPubKey: "",
    signatureVersion: 0,
    onionPath: @[]
  )

  echo "[demo] publish order id=", order.id
  discard await traderDex.publishOrder(order)
  await sleepAsync(2_000)

  if traderDex.klineStore != nil:
    let buckets = await traderDex.klineStore.snapshot()
    echo &"[demo] kline buckets={buckets.len}"

  await traderSw.stop()
  await matcherSw.stop()

when isMainModule:
  waitFor(main())
