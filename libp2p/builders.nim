# Nim-Libp2p
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

## This module contains a Switch Building helper.
runnableExamples:
  let switch = SwitchBuilder.new().withRng(rng).withAddresses(multiaddress)
    # etc
    .build()

{.push raises: [].}

import options, tables, chronos, chronicles, sequtils, strutils, stew/byteutils
import features
import
  switch,
  peerid,
  peerinfo,
  stream/connection,
  multiaddress,
  crypto/crypto,
  transports/[transport, tcptransport, memorytransport],
  muxers/[muxer, mplex/mplex, yamux/yamux],
  protocols/[identify, secure/secure, secure/noise]
when not defined(libp2p_no_tls):
  import protocols/secure/tls
  import transports/wstransport
import protocols/[
    rendezvous,
    datatransfer/datatransfer,
    datatransfer/channelmanager,
    datatransfer/graphsyncadapter,
    datatransfer/persistence,
    datatransfer/protobuf,
    bitswap/bitswap,
    bitswap/store,
    graphsync/graphsync,
    fetch/fetch,
    http/http as lpHttp,
    pubsub/gossipsub/types,
    pubsub/gossipsub,
    pubsub/episub/episub,
  ],
  pnet,
  protocols/connectivity/[
    autonat/server,
    autonat/client,
    autonat/service,
    autonatv2/server,
    autonatv2/service,
    autonatv2/client,
    relay/relay,
    relay/client,
    relay/rtransport,
  ],
  connmanager,
  upgrademngrs/muxedupgrade,
  observedaddrmanager,
  autotls/service,
  nameresolving/nameresolver,
  resourcemanager,
  memorymanager,
  errors,
  utility,
  connectiongater,
  bandwidthmanager,
  delegatedrouting,
  delegatedrouting/[store, server],
  providers/bitswapadvertiser
import services/[wildcardresolverservice, metricsservice, otelmetricsservice,
  noderesourceservice, mobilemeshservice, synccastcontrolservice,
  distributedinferenceservice, autorelayservice, hpservice, wanbootstrapservice]
when libp2pDataTransferEnabled:
  import services/[otellogsservice, oteltracesservice]

let defaultHttpNotFoundHandler*: lpHttp.HttpHandler =
  proc(_: lpHttp.HttpRequest): Future[lpHttp.HttpResponse] {.gcsafe.} =
    let fut = newFuture[lpHttp.HttpResponse]()
    fut.complete(
      lpHttp.HttpResponse(
        status: 404,
        headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
        body: "not found".toBytes(),
      )
    )
    fut

export
  switch, peerid, peerinfo, connection, multiaddress, crypto, errors, ServerFlags, PrivateNetworkKey, loadPrivateNetworkKey
when not defined(libp2p_no_tls):
  export TLSPrivateKey, TLSCertificate, TLSFlags
else:
  type
    TLSPrivateKey* = ref object
    TLSCertificate* = ref object
    TLSFlags* = enum
      NoVerifyHost
      NoVerifyServerName

logScope:
  topics = "libp2p builder"

const MemoryAutoAddress* = memorytransport.MemoryAutoAddress

when libp2pDataTransferEnabled:
  type GraphSyncAdapterSettings = object
    transferConfig: GraphSyncTransferConfig
    hook: GraphSyncFetchHook
    store: BitswapBlockStore
    persistence: DataTransferChannelPersistence
    eventHandler: DataTransferEventHandler

when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
  type WebtransportRotationSettings = object
    interval: Duration
    keepHistory: int

type
  TransportProvider* {.deprecated: "Use TransportBuilder instead".} =
    proc(upgr: Upgrade, privateKey: PrivateKey): Transport {.gcsafe, raises: [].}

  TransportBuilder* {.public.} =
    proc(config: TransportConfig): Transport {.gcsafe, raises: [].}

  TransportConfig* = ref object
    upgr*: Upgrade
    privateKey*: PrivateKey
    autotls*: Opt[AutotlsService]

  SecureProtocol* {.pure.} = enum
    Noise,
    Tls

  SwitchBuilder* = ref object
    privKey: Opt[PrivateKey]
    addresses: seq[MultiAddress]
    secureManagers: seq[SecureProtocol]
    allowImplicitSecureDefault: bool
    muxers: seq[MuxerProvider]
    transports: seq[TransportBuilder]
    rng: ref HmacDrbgContext
    maxConnections: int
    maxIn: int
    sendSignedPeerRecord: bool
    maxOut: int
    maxConnsPerPeer: int
    protoVersion: string
    agentVersion: string
    nameResolver: NameResolver
    peerStoreCapacity: Opt[int]
    autonat: bool
    autonatV2ServerConfig: Opt[AutonatV2Config]
    autonatV2Client: AutonatV2Client
    autonatV2ServiceConfig: AutonatV2ServiceConfig
    autotls: Opt[AutotlsService]
    circuitRelay: Opt[Relay]
    rdv: Opt[RendezVous]
    services: seq[Service]
    bitswapService: BitswapService
    graphSyncService: GraphSyncService
    fetchService: FetchService
    when libp2pDataTransferEnabled:
      dataTransferService: DataTransferService
      graphSyncAdapterSettings: Opt[GraphSyncAdapterSettings]
    observedAddrManager: ObservedAddrManager
    enableWildcardResolver: bool
    connectionGater: ConnectionGater
    pnetProtector: ConnectionProtector
    bandwidthAlpha: float
    bandwidthLimits: BandwidthLimitConfig
    resourceManager: ResourceManager
    resourceManagerConfig: Opt[ResourceManagerConfig]
    memoryManager: MemoryManager
    memoryLimits: MemoryLimitConfig
    delegatedRouting: DelegatedRoutingClient
    httpService: lpHttp.HttpService
    delegatedRoutingStore: DelegatedRoutingStore
    delegatedRoutingServer: DelegatedRoutingServer
    delegatedRoutingRoutesRegistered: bool
    rendezVousPolicies: Table[string, NamespacePolicy]
    when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
      webtransportRotation: Opt[WebtransportRotationSettings]
      when defined(libp2p_msquic_experimental):
        msQuicCerthashHistory: seq[string]

proc new*(T: type[SwitchBuilder]): T {.public.} =
  ## Creates a SwitchBuilder

  let address =
    MultiAddress.init("/ip4/127.0.0.1/tcp/0").expect("Should initialize to default")

  var builder = SwitchBuilder(
    privKey: Opt.none(PrivateKey),
    addresses: @[address],
    secureManagers: @[],
    allowImplicitSecureDefault: true,
    muxers: @[],
    transports: @[],
    rng: nil,
    maxConnections: MaxConnections,
    maxIn: -1,
    sendSignedPeerRecord: false,
    maxOut: -1,
    maxConnsPerPeer: MaxConnectionsPerPeer,
    protoVersion: ProtoVersion,
    agentVersion: AgentVersion,
    nameResolver: nil,
    peerStoreCapacity: Opt.none(int),
    autonat: false,
    autonatV2ServerConfig: Opt.none(AutonatV2Config),
    autonatV2Client: nil,
    autonatV2ServiceConfig: AutonatV2ServiceConfig.new(),
    autotls: Opt.none(AutotlsService),
    circuitRelay: Opt.none(Relay),
    rdv: Opt.none(RendezVous),
    services: @[],
    bitswapService: nil,
    graphSyncService: nil,
    fetchService: nil,
    observedAddrManager: nil,
    enableWildcardResolver: true,
    connectionGater: nil,
    pnetProtector: nil,
    bandwidthAlpha: DefaultBandwidthAlpha,
    bandwidthLimits: BandwidthLimitConfig.init(),
    resourceManager: nil,
    resourceManagerConfig: Opt.none(ResourceManagerConfig),
    memoryManager: nil,
    memoryLimits: MemoryLimitConfig.init(),
    delegatedRouting: nil,
    httpService: nil,
    delegatedRoutingStore: nil,
    delegatedRoutingServer: nil,
    delegatedRoutingRoutesRegistered: false,
    rendezVousPolicies: initTable[string, NamespacePolicy]()
  )
  when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
    builder.webtransportRotation = Opt.none(WebtransportRotationSettings)
    when defined(libp2p_msquic_experimental):
      builder.msQuicCerthashHistory = @[]
  when libp2pDataTransferEnabled:
    builder.dataTransferService = nil
    builder.graphSyncAdapterSettings = Opt.none(GraphSyncAdapterSettings)
  builder

proc withPrivateKey*(
    b: SwitchBuilder, privateKey: PrivateKey
): SwitchBuilder {.public.} =
  ## Set the private key of the switch. Will be used to
  ## generate a PeerId

  b.privKey = Opt.some(privateKey)
  b

proc withAddresses*(
    b: SwitchBuilder, addresses: seq[MultiAddress], enableWildcardResolver: bool = true
): SwitchBuilder {.public.} =
  ## | Set the listening addresses of the switch
  ## | Calling it multiple time will override the value
  b.addresses = addresses
  b.enableWildcardResolver = enableWildcardResolver
  b

proc withAddress*(
    b: SwitchBuilder, address: MultiAddress, enableWildcardResolver: bool = true
): SwitchBuilder {.public.} =
  ## | Set the listening address of the switch
  ## | Calling it multiple time will override the value
  b.withAddresses(@[address], enableWildcardResolver)

proc withSignedPeerRecord*(b: SwitchBuilder, sendIt = true): SwitchBuilder {.public.} =
  b.sendSignedPeerRecord = sendIt
  b

proc withMplex*(
    b: SwitchBuilder, inTimeout = 5.minutes, outTimeout = 5.minutes, maxChannCount = 200
): SwitchBuilder {.public.} =
  ## | Uses `Mplex <https://docs.libp2p.io/concepts/stream-multiplexing/#mplex>`_ as a multiplexer
  ## | `Timeout` is the duration after which a inactive connection will be closed
  proc newMuxer(conn: Connection): Muxer =
    Mplex.new(conn, inTimeout, outTimeout, maxChannCount)

  assert b.muxers.countIt(it.codec == MplexCodec) == 0, "Mplex build multiple times"
  b.muxers.add(MuxerProvider.new(newMuxer, MplexCodec))
  b

proc withYamux*(
    b: SwitchBuilder,
    maxChannCount: int = MaxChannelCount,
    windowSize: int = YamuxDefaultWindowSize,
    maxSendQueueSize: int = 256000,
    inTimeout: Duration = 5.minutes,
    outTimeout: Duration = 5.minutes,
): SwitchBuilder =
  proc newMuxer(conn: Connection): Muxer =
    Yamux.new(
      conn,
      maxChannCount = maxChannCount,
      windowSize = windowSize,
      maxSendQueueSize = maxSendQueueSize,
      inTimeout = inTimeout,
      outTimeout = outTimeout,
    )

  assert b.muxers.countIt(it.codec == YamuxCodec) == 0, "Yamux build multiple times"
  b.muxers.add(MuxerProvider.new(newMuxer, YamuxCodec))
  b

proc withNoise*(b: SwitchBuilder): SwitchBuilder {.public.} =
  if SecureProtocol.Noise notin b.secureManagers:
    b.secureManagers.add(SecureProtocol.Noise)
  b

when not defined(libp2p_no_tls):
  proc withTls*(b: SwitchBuilder): SwitchBuilder {.public.} =
    if SecureProtocol.Tls notin b.secureManagers:
      b.secureManagers.add(SecureProtocol.Tls)
    b

proc withSecureManagers*(
    b: SwitchBuilder, managers: openArray[SecureProtocol]
): SwitchBuilder {.public.} =
  b.secureManagers = @managers
  b

proc withPnet*(
    b: SwitchBuilder, key: PrivateNetworkKey
): SwitchBuilder {.public, raises: [LPError].} =
  when defined(libp2p_pnet_disable):
    raise newException(
      LPError, "libp2p built without pnet support (-d:libp2p_pnet_disable)"
    )
  else:
    if b.rng.isNil:
      b.rng = newRng()
    if b.rng.isNil:
      raise newException(LPError, "failed to initialize RNG for pnet protector")
    b.pnetProtector = newConnectionProtector(key, b.rng)
    info "configured private network key", keyId = b.pnetProtector.keyId
    b

proc withPnetFromFile*(
    b: SwitchBuilder, path: string
): SwitchBuilder {.public, raises: [LPError].} =
  when defined(libp2p_pnet_disable):
    raise newException(
      LPError, "libp2p built without pnet support (-d:libp2p_pnet_disable)"
    )
  else:
    let key = loadPrivateNetworkKey(path).valueOr:
      raise newException(LPError, "failed loading pnet key: " & error)
    b.withPnet(key)

proc withPnetFromString*(
    b: SwitchBuilder, content: string
): SwitchBuilder {.public, raises: [LPError].} =
  when defined(libp2p_pnet_disable):
    raise newException(
      LPError, "libp2p built without pnet support (-d:libp2p_pnet_disable)"
    )
  else:
    let key = loadPrivateNetworkKeyFromString(content).valueOr:
      raise newException(LPError, "failed parsing pnet key: " & error)
    b.withPnet(key)

proc withConnectionGater*(
    b: SwitchBuilder, gater: ConnectionGater
): SwitchBuilder {.public.} =
  b.connectionGater = gater
  b

proc withTransport*(
    b: SwitchBuilder, prov: TransportBuilder
): SwitchBuilder {.public.} =
  ## Use a custom transport
  runnableExamples:
    let switch = SwitchBuilder
      .new()
      .withTransport(
        proc(config: TransportConfig): Transport =
          TcpTransport.new(flags, config.upgr)
      )
      .build()
  b.transports.add(prov)
  b

proc withoutImplicitSecureDefault*(b: SwitchBuilder): SwitchBuilder {.public.} =
  b.allowImplicitSecureDefault = false
  b

proc withTransport*(
    b: SwitchBuilder, prov: TransportProvider
): SwitchBuilder {.deprecated: "Use TransportBuilder instead".} =
  ## Use a custom transport
  runnableExamples:
    let switch = SwitchBuilder
      .new()
      .withTransport(
        proc(upgr: Upgrade, privateKey: PrivateKey): Transport =
          TcpTransport.new(flags, upgr)
      )
      .build()
  let tBuilder: TransportBuilder = proc(config: TransportConfig): Transport =
    prov(config.upgr, config.privateKey)
  b.withTransport(tBuilder)

proc withTcpTransport*(
    b: SwitchBuilder, flags: set[ServerFlags] = {}
): SwitchBuilder {.public.} =
  b.withTransport(
    proc(config: TransportConfig): Transport =
      TcpTransport.new(flags, config.upgr)
  )

when not defined(libp2p_no_tls):
  proc withWsTransport*(
      b: SwitchBuilder,
      tlsPrivateKey: TLSPrivateKey = nil,
      tlsCertificate: TLSCertificate = nil,
      tlsFlags: set[TLSFlags] = {},
      flags: set[ServerFlags] = {},
  ): SwitchBuilder =
    b.withTransport(
      proc(config: TransportConfig): Transport =
        WsTransport.new(
          config.upgr, tlsPrivateKey, tlsCertificate, config.autotls, tlsFlags, flags
        )
    )

when defined(libp2p_quic_support) and not defined(libp2p_msquic_experimental):
  {.error: "libp2p_quic_support has been removed. Enable -d:libp2p_msquic_experimental and use MsQuic transport.".}

when defined(libp2p_msquic_experimental):
  import transports/[msquictransport, quicruntime]
  export quicruntime.QuicRuntimePreference
  export quicruntime.QuicRuntimeBuiltinPolicy

  type
    MsQuicTransportHook* = proc(transport: MsQuicTransport) {.gcsafe, raises: [].}
    MsQuicTransportBuilderConfig* = object
      config*: MsQuicTransportConfig
      onTransport*: Opt[MsQuicTransportHook]
      certhashHistory*: seq[string]

  proc init*(_: type MsQuicTransportBuilderConfig): MsQuicTransportBuilderConfig =
    MsQuicTransportBuilderConfig(
      config: MsQuicTransportConfig(),
      onTransport: Opt.none(MsQuicTransportHook),
      certhashHistory: @[]
    )

  proc withWebtransportCerthashHistory*(
      cfg: var MsQuicTransportBuilderConfig, history: openArray[string]
  ) {.public.} =
    cfg.certhashHistory.setLen(0)
    for hash in history:
      cfg.certhashHistory.add(hash)

  proc useAutoRuntime*(cfg: var MsQuicTransportBuilderConfig) {.public.} =
    quicruntime.useAutoRuntime(cfg.config)

  proc useNativeRuntime*(cfg: var MsQuicTransportBuilderConfig) {.public.} =
    quicruntime.useNativeRuntime(cfg.config)

  proc preferBuiltinRuntime*(cfg: var MsQuicTransportBuilderConfig) {.public.} =
    quicruntime.preferBuiltinRuntime(cfg.config)

  proc useBuiltinRuntime*(cfg: var MsQuicTransportBuilderConfig) {.public.} =
    quicruntime.useBuiltinRuntime(cfg.config)

  proc withMsQuicTransport*(b: SwitchBuilder): SwitchBuilder {.public.} =
    b.withTransport(
      proc(config: TransportConfig): Transport =
        let transport =
          newMsQuicTransport(config.upgr, config.privateKey, MsQuicTransportConfig())
        if b.msQuicCerthashHistory.len > 0:
          transport.loadWebtransportCerthashHistory(b.msQuicCerthashHistory)
        transport
    )

  proc withMsQuicWebtransportCerthashHistory*(
      b: SwitchBuilder, history: openArray[string]
  ): SwitchBuilder {.public.} =
    b.msQuicCerthashHistory = @[]
    for hash in history:
      b.msQuicCerthashHistory.add(hash)
    b

  proc withMsQuicTransport*(
      b: SwitchBuilder, cfg: MsQuicTransportBuilderConfig
  ): SwitchBuilder {.public.} =
    let builderCfg = cfg
    b.withTransport(
      proc(config: TransportConfig): Transport =
        let transport = newMsQuicTransport(
          config.upgr, config.privateKey, builderCfg.config
        )
        var combinedHistory: seq[string] = @[]
        if builderCfg.certhashHistory.len > 0:
          combinedHistory &= builderCfg.certhashHistory
        if b.msQuicCerthashHistory.len > 0:
          combinedHistory &= b.msQuicCerthashHistory
        if combinedHistory.len > 0:
          transport.loadWebtransportCerthashHistory(combinedHistory)
        builderCfg.onTransport.withValue(hook):
          hook(transport)
        transport
    )

  proc withQuicTransport*(b: SwitchBuilder): SwitchBuilder {.public, deprecated: "OpenSSL QUIC 已移除，请改用 withMsQuicTransport".} =
    b.withMsQuicTransport()

  proc withQuicTransport*(
      b: SwitchBuilder, cfg: MsQuicTransportBuilderConfig
  ): SwitchBuilder {.public, deprecated: "OpenSSL QUIC 已移除，请改用 withMsQuicTransport".} =
    b.withMsQuicTransport(cfg)

  const MsQuicDefaultWebtransportHistory = 2

  proc withWebtransportCertificateRotation*(
      b: SwitchBuilder,
      interval: Duration,
      keepHistory: int = MsQuicDefaultWebtransportHistory,
  ): SwitchBuilder {.public.} =
    when declared(b.webtransportRotation):
      if interval <= chronos.ZeroDuration:
        b.webtransportRotation = Opt.none(WebtransportRotationSettings)
      else:
        let sanitizedHistory = if keepHistory < 1: 1 else: keepHistory
        b.webtransportRotation = Opt.some(
          WebtransportRotationSettings(
            interval: interval, keepHistory: sanitizedHistory
          )
        )
    b

else:
  discard
when defined(libp2p_webrtc_support):
  proc withWebRtcDirectTransport*(b: SwitchBuilder): SwitchBuilder {.public.} =
    b.withTransport(
        proc(config: TransportConfig): Transport =
          WebRtcDirectTransport.new(config.upgr, config.privateKey)
      )

    proc withWebRtcDirectTransport*(
        b: SwitchBuilder, cfg: WebRtcDirectConfig
    ): SwitchBuilder {.public.} =
      b.withTransport(
        proc(config: TransportConfig): Transport =
          WebRtcDirectTransport.new(config.upgr, config.privateKey, cfg)
      )

    proc withWebRtcStarTransport*(b: SwitchBuilder): SwitchBuilder {.public.} =
      b.withTransport(
        proc(config: TransportConfig): Transport =
          WebRtcStarTransport.new(config.upgr, config.privateKey)
      )

    proc withWebRtcStarTransport*(
        b: SwitchBuilder, cfg: WebRtcDirectConfig
    ): SwitchBuilder {.public.} =
      b.withTransport(
        proc(config: TransportConfig): Transport =
          WebRtcStarTransport.new(config.upgr, config.privateKey, cfg)
      )

proc withMemoryTransport*(b: SwitchBuilder): SwitchBuilder {.public.} =
  b.withTransport(
    proc(config: TransportConfig): Transport =
      MemoryTransport.new(config.upgr)
  )

proc withRng*(b: SwitchBuilder, rng: ref HmacDrbgContext): SwitchBuilder {.public.} =
  b.rng = rng
  b

proc withMaxConnections*(
    b: SwitchBuilder, maxConnections: int
): SwitchBuilder {.public.} =
  ## Maximum concurrent connections of the switch. You should either use this, or
  ## `withMaxIn <#withMaxIn,SwitchBuilder,int>`_ & `withMaxOut<#withMaxOut,SwitchBuilder,int>`_
  b.maxConnections = maxConnections
  b

proc withMaxIn*(b: SwitchBuilder, maxIn: int): SwitchBuilder {.public.} =
  ## Maximum concurrent incoming connections. Should be used with `withMaxOut<#withMaxOut,SwitchBuilder,int>`_
  b.maxIn = maxIn
  b

proc withMaxOut*(b: SwitchBuilder, maxOut: int): SwitchBuilder {.public.} =
  ## Maximum concurrent outgoing connections. Should be used with `withMaxIn<#withMaxIn,SwitchBuilder,int>`_
  b.maxOut = maxOut
  b

proc withMaxConnsPerPeer*(
    b: SwitchBuilder, maxConnsPerPeer: int
): SwitchBuilder {.public.} =
  b.maxConnsPerPeer = maxConnsPerPeer
  b

proc withBandwidthAlpha*(b: SwitchBuilder, alpha: float): SwitchBuilder {.public.} =
  b.bandwidthAlpha = alpha
  b

proc withBandwidthLimits*(
    b: SwitchBuilder, limits: BandwidthLimitConfig
): SwitchBuilder {.public.} =
  b.bandwidthLimits = limits
  b

proc withMemoryLimits*(
    b: SwitchBuilder, limits: MemoryLimitConfig
): SwitchBuilder {.public.} =
  b.memoryLimits = limits
  b

proc withMemoryManager*(
    b: SwitchBuilder, manager: MemoryManager
): SwitchBuilder {.public.} =
  b.memoryManager = manager
  b.memoryLimits = MemoryLimitConfig.init()
  b

proc withDelegatedRoutingClient*(
    b: SwitchBuilder, client: DelegatedRoutingClient
): SwitchBuilder {.public.} =
  b.delegatedRouting = client
  b

proc withDelegatedRouting*(
    b: SwitchBuilder,
    baseUrl: string,
    userAgent: string = DefaultUserAgent,
    acceptNdjson: bool = true,
    protocolFilter: seq[string] = @[],
    addrFilter: seq[string] = @[]
): SwitchBuilder {.raises: [LPError], public.} =
  b.delegatedRouting = DelegatedRoutingClient.new(
    baseUrl,
    userAgent = userAgent,
    acceptNdjson = acceptNdjson,
    protocolFilter = protocolFilter,
    addrFilter = addrFilter,
  )
  b

proc withDelegatedRoutingServer*(
    b: SwitchBuilder,
    store: DelegatedRoutingStore = nil,
    httpConfig: lpHttp.HttpConfig = lpHttp.HttpConfig.init(),
    defaultHandler: lpHttp.HttpHandler = nil,
    persistence: DelegatedRoutingPersistence = nil,
): SwitchBuilder {.public.} =
  var routingStore = store
  if routingStore.isNil:
    routingStore = DelegatedRoutingStore.new(persistence = persistence)
  elif not persistence.isNil:
    warn "delegated routing persistence ignored because a custom store was provided"
  b.delegatedRoutingStore = routingStore

  if b.delegatedRoutingServer.isNil:
    b.delegatedRoutingServer = DelegatedRoutingServer.new(routingStore)
  else:
    b.delegatedRoutingServer.store = routingStore

  if b.httpService.isNil:
    var handler = defaultHandler
    if handler.isNil:
      handler = defaultHttpNotFoundHandler
    let svc = lpHttp.HttpService.new(handler, httpConfig)
    var baseService = cast[Service](svc)
    b.services.add(baseService)
    b.httpService = svc
  if not b.delegatedRoutingRoutesRegistered:
    b.delegatedRoutingServer.registerRoutes(b.httpService)
    b.delegatedRoutingRoutesRegistered = true

  b

proc withPeerStore*(b: SwitchBuilder, capacity: int): SwitchBuilder {.public.} =
  b.peerStoreCapacity = Opt.some(capacity)
  b

proc withProtoVersion*(
    b: SwitchBuilder, protoVersion: string
): SwitchBuilder {.public.} =
  b.protoVersion = protoVersion
  b

proc withAgentVersion*(
    b: SwitchBuilder, agentVersion: string
): SwitchBuilder {.public.} =
  b.agentVersion = agentVersion
  b

proc withNameResolver*(
    b: SwitchBuilder, nameResolver: NameResolver
): SwitchBuilder {.public.} =
  b.nameResolver = nameResolver
  b

proc withAutonat*(b: SwitchBuilder): SwitchBuilder =
  b.autonat = true
  b

proc withAutonatV2Server*(
    b: SwitchBuilder, config: AutonatV2Config = AutonatV2Config.new()
): SwitchBuilder =
  b.autonatV2ServerConfig = Opt.some(config)
  b

proc withAutonatV2*(
    b: SwitchBuilder, serviceConfig = AutonatV2ServiceConfig.new()
): SwitchBuilder =
  b.autonatV2Client = AutonatV2Client.new(b.rng)
  b.autonatV2ServiceConfig = serviceConfig
  b

when defined(libp2p_autotls_support):
  proc withAutotls*(
      b: SwitchBuilder, config: AutotlsConfig = AutotlsConfig.new()
  ): SwitchBuilder {.public.} =
    b.autotls = Opt.some(AutotlsService.new(config = config))
    b

proc withCircuitRelay*(b: SwitchBuilder, r: Relay = Relay.new()): SwitchBuilder =
  b.circuitRelay = Opt.some(r)
  b

proc hasCircuitRelay*(b: SwitchBuilder): bool {.public.} =
  not b.isNil and b.circuitRelay.isSome

proc withRendezVous*(
    b: SwitchBuilder, rdv: RendezVous = RendezVous.new()
): SwitchBuilder =
  b.rdv = Opt.some(rdv)
  b

proc withRendezVousNamespacePolicy*(
    b: SwitchBuilder, namespace: string, policy: NamespacePolicy
): SwitchBuilder {.public.} =
  ## Queue a rendezvous namespace policy to be applied during ``build``.
  b.rendezVousPolicies[namespace] = policy
  b

proc withServices*(b: SwitchBuilder, services: seq[Service]): SwitchBuilder =
  b.services = services
  b

proc withNodeResourceService*(
    b: SwitchBuilder,
    dataDir = "",
    config: NodeResourceServiceConfig = NodeResourceServiceConfig.init(),
    source: NodeResourceCollectorSource = NodeResourceCollectorSource(),
): SwitchBuilder {.public.} =
  let svc = NodeResourceService.new(dataDir = dataDir, config = config, source = source)
  b.services.keepItIf(not (it of NodeResourceService))
  b.services.add(svc)
  b

proc withMobileMeshService*(
    b: SwitchBuilder,
    config: MobileMeshServiceConfig = MobileMeshServiceConfig.init(),
): SwitchBuilder {.public.} =
  let svc = MobileMeshService.new(config = config)
  b.services.keepItIf(not (it of MobileMeshService))
  b.services.add(svc)
  b

proc withWanBootstrapService*(
    b: SwitchBuilder,
    config: WanBootstrapConfig = WanBootstrapConfig.init(),
): SwitchBuilder {.public.} =
  let svc = WanBootstrapService.new(config = config)
  b.services.keepItIf(not (it of WanBootstrapService))
  b.services.add(svc)
  b

proc withWanBootstrapProfile*(
    b: SwitchBuilder,
    config: WanBootstrapConfig = WanBootstrapConfig.init(),
    hpConfig: HPServiceConfig = HPServiceConfig.init(),
): SwitchBuilder {.public.} =
  if b.rng.isNil:
    b.rng = newRng()
  let next = b.withSignedPeerRecord(true).withWanBootstrapService(config)

  proc ensureHolePunchProfile(nextBuilder: SwitchBuilder): SwitchBuilder =
    if nextBuilder.isNil:
      return nextBuilder
    if nextBuilder.rng.isNil:
      nextBuilder.rng = newRng()
    if nextBuilder.services.anyIt(it of HPService):
      return nextBuilder

    var relayClient: RelayClient = nil
    if nextBuilder.circuitRelay.isSome and nextBuilder.circuitRelay.get() of RelayClient:
      relayClient = RelayClient(nextBuilder.circuitRelay.get())
    else:
      relayClient = RelayClient.new()
      discard nextBuilder.withCircuitRelay(relayClient)

    if relayClient.isNil or nextBuilder.rng.isNil:
      return nextBuilder

    let autoRelaySvc = AutoRelayService.new(1, relayClient, nil, nextBuilder.rng)
    let autonatSvc =
      AutonatService.new(AutonatClient(), nextBuilder.rng, enableAddressMapper = false)
    let hpSvc = HPService.new(autonatSvc, autoRelaySvc, config = hpConfig)
    nextBuilder.services.keepItIf(not (it of HPService))
    nextBuilder.services.add(hpSvc)
    nextBuilder

  case config.role
  of WanBootstrapRole.anchor:
    if next.autonatV2ServerConfig.isNone:
      discard next.withAutonatV2Server()
    if next.circuitRelay.isNone:
      discard next.withCircuitRelay(Relay.new())
  of WanBootstrapRole.publicNode:
    if next.autonatV2Client.isNil and next.autonatV2ServerConfig.isNone:
      discard next.withAutonatV2()
  of WanBootstrapRole.mobileEdge:
    if next.autonatV2Client.isNil and next.autonatV2ServerConfig.isNone:
      discard next.withAutonatV2()
    if next.circuitRelay.isNone:
      discard next.withCircuitRelay(RelayClient.new())
    discard ensureHolePunchProfile(next)
  next

proc withMobileFullP2PProfile*(
    b: SwitchBuilder,
    dataDir = "",
    resourceConfig: NodeResourceServiceConfig = NodeResourceServiceConfig.init(),
    resourceSource: NodeResourceCollectorSource = NodeResourceCollectorSource(),
    mobileMeshConfig: MobileMeshServiceConfig = MobileMeshServiceConfig.init(),
    maxNumRelays = 2,
    hpConfig: HPServiceConfig = HPServiceConfig.init(),
): SwitchBuilder {.public, raises: [LPError].} =
  if b.rng.isNil:
    b.rng = newRng()
  if b.rng.isNil:
    raise newException(LPError, "failed to initialize RNG for mobile full P2P profile")

  if b.addresses.len == 0 or
      (b.addresses.len == 1 and $b.addresses[0] == "/ip4/127.0.0.1/tcp/0"):
    discard b.withAddresses(
      @[MultiAddress.init("/ip4/0.0.0.0/udp/0/quic-v1").tryGet()]
    )
  else:
    for address in b.addresses:
      if not ($address).contains("/quic"):
        raise newException(
          LPError,
          "mobile full P2P profile requires QUIC listen addresses, got " & $address,
        )

  b.transports.setLen(0)
  when defined(libp2p_msquic_experimental):
    discard b.withMsQuicTransport()
  else:
    raise newException(
      LPError,
      "mobile full P2P profile requires QUIC support (-d:libp2p_msquic_experimental)",
    )

  discard b.withNodeResourceService(
    dataDir = dataDir, config = resourceConfig, source = resourceSource
  )
  discard b.withMobileMeshService(config = mobileMeshConfig)
  discard b.withAutonat()

  let relayClient = RelayClient.new()
  discard b.withCircuitRelay(relayClient)

  let autoRelaySvc = AutoRelayService.new(max(1, maxNumRelays), relayClient, nil, b.rng)
  let autonatSvc =
    AutonatService.new(AutonatClient(), b.rng, enableAddressMapper = false)
  let hpSvc = HPService.new(autonatSvc, autoRelaySvc, config = hpConfig)
  b.services.keepItIf(not (it of HPService))
  b.services.add(hpSvc)
  b

proc withSynccastControlService*(
    b: SwitchBuilder,
    config: SynccastControlConfig = SynccastControlConfig.init(),
): SwitchBuilder {.public.} =
  let next = b.withNodeResourceService()
  let svc = SynccastControlService.new(config = config)
  next.services.keepItIf(not (it of SynccastControlService))
  next.services.add(svc)
  next

proc withDistributedInferenceService*(
    b: SwitchBuilder,
    config: DistributedInferenceConfig = DistributedInferenceConfig.init(),
    store: BitswapBlockStore = nil,
    executor: DistributedInferenceExecutor = nil,
): SwitchBuilder {.public.} =
  let next = b.withNodeResourceService()
  let svc = DistributedInferenceService.new(
    config = config,
    store = store,
    executor = executor,
  )
  next.services.keepItIf(not (it of DistributedInferenceService))
  next.services.add(svc)
  next

proc withMetricsExporter*(
    b: SwitchBuilder,
    address: string = "127.0.0.1",
    port: Port = Port(8000),
    interval: Duration = 5.seconds,
): SwitchBuilder {.public.} =
  let svc = MetricsService.new(address = address, port = port, interval = interval)
  var baseService = cast[Service](svc)
  b.services.keepItIf(not (it of MetricsService))
  b.services.add(baseService)
  b

proc withOpenTelemetryExporter*(
    b: SwitchBuilder,
    endpoint: string,
    headers: openArray[(string, string)] = [],
    resourceAttributes: openArray[(string, string)] = [],
    scopeName: string = "nim-libp2p",
    scopeVersion: string = "",
    interval: Duration = 5.seconds,
    timeout: Duration = 3.seconds,
): SwitchBuilder {.public, raises: [ValueError].} =
  let svc = OtelMetricsService.new(
    endpoint = endpoint,
    headers = headers.toSeq(),
    resourceAttributes = resourceAttributes.toSeq(),
    scopeName = scopeName,
    scopeVersion = scopeVersion,
    interval = interval,
    timeout = timeout,
  )
  var baseService = cast[Service](svc)
  b.services.keepItIf(not (it of OtelMetricsService))
  b.services.add(baseService)
  b

when libp2pDataTransferEnabled:
  proc withOpenTelemetryLogsExporter*(
      b: SwitchBuilder,
      endpoint: string,
      headers: openArray[(string, string)] = [],
      resourceAttributes: openArray[(string, string)] = [],
      scopeName: string = "nim-libp2p",
      scopeVersion: string = "",
      flushInterval: Duration = 2.seconds,
      timeout: Duration = 3.seconds,
      maxBatchSize: int = 64,
      globalAttributes: openArray[(string, string)] = [],
      severityOverrides: openArray[DataTransferSeverityOverride] = [],
  ): SwitchBuilder {.public, raises: [ValueError].} =
    let svc = OtelLogsService.new(
      endpoint = endpoint,
      headers = headers.toSeq(),
      resourceAttributes = resourceAttributes.toSeq(),
      scopeName = scopeName,
      scopeVersion = scopeVersion,
      flushInterval = flushInterval,
      timeout = timeout,
      maxBatchSize = maxBatchSize,
      globalAttributes = globalAttributes.toSeq(),
      severityOverrides = severityOverrides.toSeq(),
    )
    var baseService = cast[Service](svc)
    b.services.keepItIf(not (it of OtelLogsService))
    b.services.add(baseService)
    b

  proc withOpenTelemetryTracesExporter*(
      b: SwitchBuilder,
      endpoint: string,
      headers: openArray[(string, string)] = [],
      resourceAttributes: openArray[(string, string)] = [],
      scopeName: string = "nim-libp2p",
      scopeVersion: string = "",
      flushInterval: Duration = 2.seconds,
      timeout: Duration = 3.seconds,
      maxBatchSize: int = 32,
      globalAttributes: openArray[(string, string)] = [],
      spanKind: string = "SPAN_KIND_INTERNAL",
  ): SwitchBuilder {.public, raises: [ValueError].} =
    let svc = OtelTracesService.new(
      endpoint = endpoint,
      headers = headers.toSeq(),
      resourceAttributes = resourceAttributes.toSeq(),
      scopeName = scopeName,
      scopeVersion = scopeVersion,
      flushInterval = flushInterval,
      timeout = timeout,
      maxBatchSize = maxBatchSize,
      globalAttributes = globalAttributes.toSeq(),
      spanKind = spanKind,
    )
    var baseService = cast[Service](svc)
    b.services.keepItIf(not (it of OtelTracesService))
    b.services.add(baseService)
    b

proc withBitswap*(
    b: SwitchBuilder,
    provider: BitswapBlockProvider,
    config: BitswapConfig = BitswapConfig.init(),
): SwitchBuilder {.public.} =
  if not b.bitswapService.isNil:
    discard
  let svc = BitswapService.new(provider = provider, config = config)
  b.bitswapService = svc
  b

proc withBitswap*(
    b: SwitchBuilder,
    store: BitswapBlockStore,
    config: BitswapConfig = BitswapConfig.init(),
): SwitchBuilder {.public.} =
  if store.isNil():
    raise newException(Defect, "bitswap store must not be nil")
  if not b.bitswapService.isNil:
    discard
  let svc = BitswapService.new(store = store, config = config)
  b.bitswapService = svc
  b

proc withGraphSync*(
    b: SwitchBuilder,
    provider: GraphSyncBlockProvider,
    config: GraphSyncConfig = GraphSyncConfig.init(),
): SwitchBuilder {.public.} =
  let svc = GraphSyncService.new(provider = provider, config = config)
  b.graphSyncService = svc
  b

proc withGraphSync*(
    b: SwitchBuilder,
    store: BitswapBlockStore,
    config: GraphSyncConfig = GraphSyncConfig.init(),
): SwitchBuilder {.public.} =
  if store.isNil():
    raise newException(Defect, "graphsync store must not be nil")
  let svc = GraphSyncService.new(store = store, config = config)
  b.graphSyncService = svc
  b

when libp2pFetchEnabled:
  proc withFetch*(
      b: SwitchBuilder, handler: FetchHandler, config: FetchConfig = FetchConfig.init()
  ): SwitchBuilder {.public.} =
    let svc = FetchService.new(handler, config)
    b.fetchService = svc
    b
else:
  proc withFetch*(
      b: SwitchBuilder, handler: FetchHandler, config: FetchConfig = FetchConfig.init()
  ): SwitchBuilder {.public.} =
    raise newException(
      Defect, disabledFeatureMessage("Fetch", "libp2p_disable_fetch")
    )

when libp2pDataTransferEnabled:
  proc withDataTransfer*(
      b: SwitchBuilder,
      handler: DataTransferHandler,
      config: DataTransferConfig = DataTransferConfig.init(),
  ): SwitchBuilder {.public.} =
    let svc = DataTransferService.new(handler, config)
    b.dataTransferService = svc
    b

  proc withGraphSyncDataTransfer*(
      b: SwitchBuilder,
      dataTransferConfig: DataTransferConfig = DataTransferConfig.init(),
      transferConfig: GraphSyncTransferConfig = GraphSyncTransferConfig.init(),
      persistence: DataTransferChannelPersistence = nil,
      hook: GraphSyncFetchHook = nil,
      eventHandler: DataTransferEventHandler = nil,
      store: BitswapBlockStore = nil,
      graphSyncConfig: GraphSyncConfig = GraphSyncConfig.init(),
  ): SwitchBuilder {.public.} =
    if b.graphSyncService.isNil:
      if store.isNil():
        raise newException(
          Defect,
          "withGraphSyncDataTransfer requires a GraphSync service or a block store",
        )
      discard b.withGraphSync(store, graphSyncConfig)
    if b.graphSyncService.isNil:
      raise newException(Defect, "failed to initialize graphsync service")

    if b.dataTransferService.isNil:
      let placeholder: DataTransferHandler =
        proc(
            conn: Connection, message: DataTransferMessage
        ): Future[Option[DataTransferMessage]] {.closure, gcsafe.} =
          trace "data transfer message queued before graphsync adapter setup",
            peer = conn.peerId, transferId = message.transferId
          let fut = newFuture[Option[DataTransferMessage]]()
          fut.complete(none(DataTransferMessage))
          fut
      discard b.withDataTransfer(placeholder, dataTransferConfig)

    let settings = GraphSyncAdapterSettings(
      transferConfig: transferConfig,
      hook: hook,
      store: store,
      persistence: persistence,
      eventHandler: eventHandler,
    )
    b.graphSyncAdapterSettings = Opt.some(settings)
    b
else:
  proc withDataTransfer*(
      b: SwitchBuilder,
      handler: DataTransferHandler,
      config: DataTransferConfig = DataTransferConfig.init(),
  ): SwitchBuilder {.public.} =
    raise newException(
      Defect, disabledFeatureMessage("Data Transfer", "libp2p_disable_datatransfer")
    )

when libp2pHttpEnabled:
  proc withHttpService*(
      b: SwitchBuilder,
      defaultHandler: lpHttp.HttpHandler,
      routes: openArray[(string, lpHttp.HttpHandler)] = [],
      config: lpHttp.HttpConfig = lpHttp.HttpConfig.init(),
  ): SwitchBuilder {.public.} =
    if b.httpService.isNil:
      let svc = lpHttp.HttpService.new(defaultHandler, config)
      for (path, handler) in routes:
        svc.registerRoute(path, handler)
      b.httpService = svc
    else:
      let svc = b.httpService
      svc.setDefaultHandler(defaultHandler)
      for (path, handler) in routes:
        svc.registerRoute(path, handler)
    b
else:
  proc withHttpService*(
      b: SwitchBuilder,
      defaultHandler: lpHttp.HttpHandler,
      routes: openArray[(string, lpHttp.HttpHandler)] = [],
      config: lpHttp.HttpConfig = lpHttp.HttpConfig.init(),
  ): SwitchBuilder {.public.} =
    raise newException(
      Defect, disabledFeatureMessage("HTTP service", "libp2p_disable_http")
    )

proc withObservedAddrManager*(
    b: SwitchBuilder, observedAddrManager: ObservedAddrManager
): SwitchBuilder =
  b.observedAddrManager = observedAddrManager
  b

proc withResourceManager*(
    b: SwitchBuilder, config: ResourceManagerConfig = ResourceManagerConfig.init()
): SwitchBuilder {.public.} =
  ## Enable the Resource Manager with the supplied configuration.
  ##
  ## Passing a custom configuration allows controlling per-connection,
  ## per-peer and per-protocol limits.
  b.resourceManagerConfig = Opt.some(config)
  b.resourceManager = nil
  b

proc withResourceManager*(
    b: SwitchBuilder, manager: ResourceManager
): SwitchBuilder {.public.} =
  ## Provide a pre-configured Resource Manager instance.
  ##
  ## When set, the builder will reuse the provided manager instead of
  ## constructing a new one from configuration.
  b.resourceManager = manager
  b.resourceManagerConfig = Opt.none(ResourceManagerConfig)
  b

proc build*(b: SwitchBuilder): Switch {.raises: [LPError], public.} =
  if b.rng == nil: # newRng could fail
    raise newException(Defect, "Cannot initialize RNG")

  let pkRes = PrivateKey.random(b.rng[])
  let seckey = b.privKey.get(otherwise = pkRes.expect("Expected default Private Key"))

  if b.secureManagers.len == 0 and b.allowImplicitSecureDefault:
    debug "no secure managers defined. Adding noise by default"
    b.secureManagers.add(SecureProtocol.Noise)

  if forcePrivateNetworkEnabled() and b.pnetProtector.isNil:
    raise (ref LPError)(
      msg: "LIBP2P_FORCE_PNET=1 requires configuring a private network PSK"
    )

  var secureManagerInstances: seq[Secure]
  let muxerCodecs = b.muxers.mapIt(it.codec)
  for secureProtocol in b.secureManagers:
    case secureProtocol
    of SecureProtocol.Noise:
      secureManagerInstances.add(
        Noise.new(b.rng, seckey, supportedMuxers = muxerCodecs).Secure
      )
    of SecureProtocol.Tls:
      when not defined(libp2p_no_tls):
        secureManagerInstances.add(TLS.new(b.rng, seckey).Secure)

  if b.muxers.len > 0:
    let alpnMuxers = b.muxers.mapIt(it.codec).filterIt(it.len > 0)
    if alpnMuxers.len > 0:
      for secureInstance in secureManagerInstances.mitems():
        when not defined(libp2p_no_tls):
          if secureInstance of TLS:
            let tls = TLS(secureInstance)
            var alpnList = alpnMuxers
            if Libp2pAlpn notin alpnList:
              alpnList.add(Libp2pAlpn)
            tls.setAlpn(alpnList)

  let peerInfo = PeerInfo.new(
    seckey, b.addresses, protoVersion = b.protoVersion, agentVersion = b.agentVersion
  )

  let connManager =
    ConnManager.new(b.maxConnsPerPeer, b.maxConnections, b.maxIn, b.maxOut)
  var resourceManager = b.resourceManager
  if resourceManager.isNil:
    b.resourceManagerConfig.withValue(config):
      resourceManager = ResourceManager.new(config)

  var memoryMgr = b.memoryManager
  if memoryMgr.isNil and b.memoryLimits.hasLimits():
    memoryMgr = MemoryManager.new(b.memoryLimits)

  if not resourceManager.isNil:
    connManager.setResourceManager(resourceManager)

  let ms = MultistreamSelect.new(resourceManager)
  let muxedUpgrade =
    MuxedUpgrade.new(b.muxers, secureManagerInstances, ms, b.pnetProtector)
  let bandwidthMgr = BandwidthManager.new(b.bandwidthAlpha, b.bandwidthLimits)

  let identify =
    if b.observedAddrManager != nil:
      Identify.new(
        peerInfo, b.sendSignedPeerRecord, b.observedAddrManager, bandwidthMgr
      )
    else:
      Identify.new(
        peerInfo, b.sendSignedPeerRecord, bandwidthManager = bandwidthMgr
      )

  muxedUpgrade.setBandwidthManager(bandwidthMgr)
  muxedUpgrade.setMemoryManager(memoryMgr)

  b.autotls.withValue(autotlsService):
    b.services.insert(autotlsService, 0)

  let transports = block:
    var transports: seq[Transport]
    for tProvider in b.transports:
      transports.add(
        tProvider(
          TransportConfig(upgr: muxedUpgrade, privateKey: seckey, autotls: b.autotls)
        )
      )
    transports

  if b.secureManagers.len == 0:
    b.secureManagers &= SecureProtocol.Noise

  if isNil(b.rng):
    b.rng = newRng()

  let peerStore = block:
    b.peerStoreCapacity.withValue(capacity):
      PeerStore.new(identify, capacity)
    else:
      PeerStore.new(identify)

  if b.enableWildcardResolver:
    b.services.insert(WildcardAddressResolverService.new(), 0)

  if not isNil(b.autonatV2Client):
    b.services.add(
      AutonatV2Service.new(
        b.rng, client = b.autonatV2Client, config = b.autonatV2ServiceConfig
      )
    )

  if b.rdv.isNone and b.rendezVousPolicies.len > 0:
    try:
      b.rdv = Opt.some(RendezVous.new())
    except RendezVousError as exc:
      raise newException(LPError, "failed to initialize rendezvous: " & exc.msg)

  let switch = newSwitch(
    peerInfo = peerInfo,
    transports = transports,
    secureManagers = secureManagerInstances,
    connManager = connManager,
    ms = ms,
    nameResolver = b.nameResolver,
    peerStore = peerStore,
    services = b.services,
    connectionGater = b.connectionGater,
    bandwidthManager = bandwidthMgr,
    resourceManager = resourceManager,
    memoryManager = memoryMgr,
    delegatedRouting = b.delegatedRouting,
    delegatedRoutingStore = b.delegatedRoutingStore,
    bitswap = b.bitswapService,
  )

  switch.mount(identify)

  if not switch.bitswap.isNil:
    switch.mount(switch.bitswap)

  if not isNil(b.autonatV2Client):
    b.autonatV2Client.setup(switch)
    switch.mount(b.autonatV2Client)

  b.autonatV2ServerConfig.withValue(config):
    switch.mount(AutonatV2.new(switch, config = config))

  if b.autonat:
    switch.mount(Autonat.new(switch))

  b.circuitRelay.withValue(relay):
    if relay of RelayClient:
      switch.addTransport(RelayTransport.new(RelayClient(relay), muxedUpgrade))
    relay.setup(switch)
    switch.mount(relay)

  b.rdv.withValue(rdvService):
    if b.rendezVousPolicies.len > 0:
      for ns, policy in b.rendezVousPolicies.pairs():
        rdvService.setNamespacePolicy(ns, policy)
    rdvService.setup(switch)
    switch.mount(rdvService)

  if not switch.bitswap.isNil:
    let store = switch.bitswap.store
    if store of AutoProviderBlockStore:
      AutoProviderBlockStore(store).attachSwitch(switch)

  when libp2pDataTransferEnabled:
    if b.graphSyncAdapterSettings.isSome:
      let settings = b.graphSyncAdapterSettings.get()
      if b.dataTransferService.isNil:
        raise newException(
          LPError, "graphsync data transfer requested but data transfer service missing"
        )
      if b.graphSyncService.isNil:
        raise newException(
          LPError, "graphsync data transfer requested but graphsync service missing"
        )
      let dialer: DataTransferDialer = proc(
          peer: PeerId, protos: seq[string]
      ): Future[Connection] {.gcsafe, raises: [DialFailedError, CancelledError].} =
        switch.dial(peer, protos)
      let manager = DataTransferChannelManager.new(
        dialer, persistence = settings.persistence
      )
      switch.dataTransferManager = manager
      var adapterStore = settings.store
      if adapterStore.isNil():
        adapterStore = b.graphSyncService.store
      let adapter = newGraphSyncDataTransfer(
        switch,
        manager,
        b.graphSyncService,
        blockStore = adapterStore,
        config = settings.transferConfig,
        eventHandler = settings.eventHandler,
        fetchHook = settings.hook,
      )
      b.dataTransferService.messageHandler = adapter.handler()
      for svc in switch.services:
        if svc of OtelLogsService:
          OtelLogsService(svc).registerDataTransferManager(manager)
        if svc of OtelTracesService:
          OtelTracesService(svc).registerDataTransferManager(manager)

  if not b.graphSyncService.isNil:
    switch.mount(b.graphSyncService)

  when libp2pDataTransferEnabled:
    if not b.dataTransferService.isNil:
      switch.mount(b.dataTransferService)

  when libp2pFetchEnabled:
    if not b.fetchService.isNil:
      switch.mount(b.fetchService)

  when libp2pHttpEnabled:
    if not b.httpService.isNil:
      switch.mount(b.httpService)

  when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
    b.webtransportRotation.withValue(rot):
      switch.webtransportRotationInterval = rot.interval
      switch.webtransportRotationKeepHistory = rot.keepHistory

  return switch

type TransportType* {.pure.} = enum
  QUIC
  TCP
  Memory

proc newStandardSwitchBuilder*(
    privKey = Opt.none(PrivateKey),
    addrs: MultiAddress | seq[MultiAddress] = newSeq[MultiAddress](),
    transport: TransportType = TransportType.TCP,
    transportFlags: set[ServerFlags] = {},
    rng = newRng(),
    secureManagers: openArray[SecureProtocol] = [
      SecureProtocol.Noise, SecureProtocol.Tls
    ],
    inTimeout: Duration = 5.minutes,
    outTimeout: Duration = 5.minutes,
    maxConnections = MaxConnections,
    maxIn = -1,
    maxOut = -1,
    maxConnsPerPeer = MaxConnectionsPerPeer,
    nameResolver = Opt.none(NameResolver),
    sendSignedPeerRecord = true,
    peerStoreCapacity = 1000,
    fetchHandler: Opt[FetchHandler] = Opt.none(FetchHandler),
    fetchConfig: FetchConfig = FetchConfig.init(),
    httpDefaultHandler: Opt[lpHttp.HttpHandler] = Opt.none(lpHttp.HttpHandler),
    httpRoutes: seq[(string, lpHttp.HttpHandler)] = @[],
    httpConfig: lpHttp.HttpConfig = lpHttp.HttpConfig.init(),
): SwitchBuilder {.raises: [LPError], public.} =
  ## Helper for common switch configurations.
  var b = SwitchBuilder
    .new()
    .withRng(rng)
    .withSignedPeerRecord(sendSignedPeerRecord)
    .withMaxConnections(maxConnections)
    .withMaxIn(maxIn)
    .withMaxOut(maxOut)
    .withMaxConnsPerPeer(maxConnsPerPeer)
    .withPeerStore(capacity = peerStoreCapacity)
    .withSecureManagers(secureManagers)

  privKey.withValue(pkey):
    b = b.withPrivateKey(pkey)

  nameResolver.withValue(nr):
    b = b.withNameResolver(nr)

  var addrs =
    when addrs is MultiAddress:
      @[addrs]
    else:
      addrs

  case transport
  of TransportType.QUIC:
    when defined(libp2p_msquic_experimental):
      if addrs.len == 0:
        addrs = @[MultiAddress.init("/ip4/0.0.0.0/udp/0/quic-v1").tryGet()]
      b = b.withMsQuicTransport().withAddresses(addrs).withMplex(
        inTimeout, outTimeout
      )
    else:
      raiseAssert "QUIC not supported in this build"
  of TransportType.TCP:
    if addrs.len == 0:
      addrs = @[MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet()]
    b = b.withTcpTransport(transportFlags).withAddresses(addrs).withMplex(
        inTimeout, outTimeout
      )
  of TransportType.Memory:
    if addrs.len == 0:
      addrs = @[MultiAddress.init(MemoryAutoAddress).tryGet()]
    b = b.withMemoryTransport().withAddresses(addrs).withMplex(inTimeout, outTimeout)

  fetchHandler.withValue(fetchCb):
    b = b.withFetch(fetchCb, fetchConfig)

  httpDefaultHandler.withValue(defaultHttpHandler):
    b = b.withHttpService(defaultHttpHandler, httpRoutes, httpConfig)

  b

when libp2pEpisubEnabled:
  proc buildWithEpisub*(
      b: SwitchBuilder,
      params: GossipSubParams = GossipSubParams.init(),
      epiParams: EpisubParams = defaultParams(),
  ): tuple[switch: Switch, router: Episub] {.public, raises: [LPError, InitializationError].} =
    let sw = b.build()
    let router = Episub.new(sw, params = params, epiParams = epiParams)
    (sw, router)
else:
  proc buildWithEpisub*(
      b: SwitchBuilder,
      params: GossipSubParams = GossipSubParams.init(),
      epiParams: EpisubParams = defaultParams(),
  ): tuple[switch: Switch, router: Episub] {.public, raises: [LPError, InitializationError].} =
    raise newException(
      Defect, disabledFeatureMessage("Episub", "libp2p_disable_episub")
    )

proc newStandardSwitch*(
    privKey = Opt.none(PrivateKey),
    addrs: MultiAddress | seq[MultiAddress] = newSeq[MultiAddress](),
    transport: TransportType = TransportType.TCP,
    transportFlags: set[ServerFlags] = {},
    rng = newRng(),
    secureManagers: openArray[SecureProtocol] = [
      SecureProtocol.Noise, SecureProtocol.Tls
    ],
    inTimeout: Duration = 5.minutes,
    outTimeout: Duration = 5.minutes,
    maxConnections = MaxConnections,
    maxIn = -1,
    maxOut = -1,
    maxConnsPerPeer = MaxConnectionsPerPeer,
    nameResolver = Opt.none(NameResolver),
    sendSignedPeerRecord = false,
    peerStoreCapacity = 1000,
    fetchHandler: Opt[FetchHandler] = Opt.none(FetchHandler),
    fetchConfig: FetchConfig = FetchConfig.init(),
    httpDefaultHandler: Opt[lpHttp.HttpHandler] = Opt.none(lpHttp.HttpHandler),
    httpRoutes: seq[(string, lpHttp.HttpHandler)] = @[],
    httpConfig: lpHttp.HttpConfig = lpHttp.HttpConfig.init(),
): Switch {.raises: [LPError], public.} =
  newStandardSwitchBuilder(
    privKey = privKey,
    addrs = addrs,
    transport = transport,
    transportFlags = transportFlags,
    rng = rng,
    secureManagers = secureManagers,
    inTimeout = inTimeout,
    outTimeout = outTimeout,
    maxConnections = maxConnections,
    maxIn = maxIn,
    maxOut = maxOut,
    maxConnsPerPeer = maxConnsPerPeer,
    nameResolver = nameResolver,
    sendSignedPeerRecord = sendSignedPeerRecord,
    peerStoreCapacity = peerStoreCapacity,
    fetchHandler = fetchHandler,
    fetchConfig = fetchConfig,
    httpDefaultHandler = httpDefaultHandler,
    httpRoutes = httpRoutes,
    httpConfig = httpConfig,
  )
  .build()
