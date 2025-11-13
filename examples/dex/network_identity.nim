# nim-libp2p 网络与身份子系统实现

import std/[json, options, sets, sequtils, strformat, strutils, tables, times]
import chronos
import libp2p
import libp2p/builders
import libp2p/peerid
import libp2p/peerinfo
import libp2p/peerstore
import libp2p/multiaddress
import libp2p/pnet
import libp2p/services/metricsservice
import libp2p/services/hpservice
import stew/byteutils

const DexIdentityCodec = "/nim-libp2p/dex/identity/1.0.0"

type
  DexRole* = enum
    drTrader      ## 普通交易终端
    drMatcher     ## 撮合/做市节点
    drRisk        ## 风控或清算节点
    drKline       ## 行情 / K 线聚合节点
    drSettlement  ## 结算 / 链上交互节点
    drObserver    ## 只读节点

  RoleSet* = HashSet[DexRole]

  DexIdentityInfo* = object
    version*: uint16
    peerId*: PeerId
    roles*: RoleSet
    labels*: Table[string, string]
    publishedAt*: Time

  DexNetworkConfig* = object
    listenAddrs*: seq[MultiAddress]
    privateNetworkKey*: Opt[PrivateNetworkKey]
    enableAutonat*: bool
    enableRelay*: bool
    metricsPort*: Opt[int]
    advertisedProtocols*: seq[string]

  DexAccessPolicy* = Table[string, RoleSet]

  DexIdentityRegistry* = ref object
    localInfo*: DexIdentityInfo
    remotes: Table[string, DexIdentityInfo]
    lock: AsyncLock

  DexIdentityProtocol* = ref object of LPProtocol
    registry: DexIdentityRegistry

  DexNetwork* = object
    switch*: Switch
    registry*: DexIdentityRegistry
    identityProtocol*: DexIdentityProtocol
    accessPolicy*: DexAccessPolicy

proc newRoleSet(vals: openArray[DexRole]): RoleSet =
  result = initHashSet[DexRole]()
  for v in vals:
    result.incl v

proc encodeIdentity(info: DexIdentityInfo): string =
  var node = newJObject()
  node["version"] = %int(info.version)
  node["peerId"] = %($info.peerId)
  var rolesArray = newJArray()
  for role in info.roles:
    rolesArray.add(%($role))
  node["roles"] = rolesArray
  var labelsNode = newJObject()
  for key, value in info.labels:
    labelsNode[key] = %value
  node["labels"] = labelsNode
  node["publishedAt"] = %toUnix(info.publishedAt)
  $node

proc parseRole(name: string): DexRole =
  try:
    parseEnum[DexRole](name)
  except ValueError:
    raise newException(ValueError, "unknown role: " & name)

proc decodeIdentity(data: string, info: var DexIdentityInfo): bool {.raises: [].} =
  try:
    let node = parseJson(data)
    let peerIdNode =
      if node.hasKey("peerId"): node["peerId"].getStr()
      else:
        return false
    let peerIdResult = PeerId.init(peerIdNode)
    if peerIdResult.isErr():
      return false
    let peerId = peerIdResult.unsafeValue
    let publishedNode =
      if node.hasKey("publishedAt"): node["publishedAt"] else: %toUnix(getTime())
    let versionVal =
      if node.hasKey("version"): node["version"].getInt()
      else: 1
    info = DexIdentityInfo(
      version: uint16(versionVal),
      peerId: peerId,
      roles: initHashSet[DexRole](),
      labels: initTable[string, string](),
      publishedAt: fromUnix(int64(publishedNode.getInt()))
    )

    if node.hasKey("roles") and node["roles"].kind == JArray:
      for roleName in node["roles"]:
        try:
          info.roles.incl(parseRole(roleName.getStr()))
        except CatchableError:
          discard

    if node.hasKey("labels") and node["labels"].kind == JObject:
      for key, value in node["labels"].pairs:
        info.labels[key] = value.getStr()
    true
  except CatchableError as exc:
    debugEcho "decode identity failed: ", exc.msg
    false

proc newDexIdentityRegistry*(localInfo: DexIdentityInfo): DexIdentityRegistry =
  DexIdentityRegistry(
    localInfo: localInfo,
    remotes: initTable[string, DexIdentityInfo](),
    lock: newAsyncLock(),
  )

proc updateRemote*(registry: DexIdentityRegistry, peerId: PeerId, info: DexIdentityInfo) {.async.} =
  try:
    await registry.lock.acquire()
  except AsyncLockError as exc:
    debugEcho "dex identity registry acquire failed: ", exc.msg
    return
  try:
    registry.remotes[$peerId] = info
  finally:
    try:
      registry.lock.release()
    except AsyncLockError:
      discard

proc getRemote*(registry: DexIdentityRegistry, peerId: PeerId): Future[Option[DexIdentityInfo]] {.async.} =
  try:
    await registry.lock.acquire()
  except AsyncLockError as exc:
    debugEcho "dex identity registry acquire failed: ", exc.msg
    return none(DexIdentityInfo)
  try:
    if registry.remotes.hasKey($peerId):
      some(registry.remotes.getOrDefault($peerId, DexIdentityInfo()))
    else:
      none(DexIdentityInfo)
  finally:
    try:
      registry.lock.release()
    except AsyncLockError:
      discard

proc listRemotes*(registry: DexIdentityRegistry): Future[seq[DexIdentityInfo]] {.async.} =
  try:
    await registry.lock.acquire()
  except AsyncLockError as exc:
    debugEcho "dex identity registry acquire failed: ", exc.msg
    return @[]
  try:
    var acc: seq[DexIdentityInfo] = @[]
    for _, value in registry.remotes.pairs:
      acc.add(value)
    result = acc
  finally:
    try:
      registry.lock.release()
    except AsyncLockError:
      discard

proc init*(proto: DexIdentityProtocol) =
  proc handle(conn: Connection, protoName: string) {.async: (raises: [CancelledError]), gcsafe, closure.} =
    try:
      let request = await conn.readLp(64 * 1024)
      if request.len > 0:
        var remoteInfo: DexIdentityInfo
        if decodeIdentity(string.fromBytes(request), remoteInfo):
          await proto.registry.updateRemote(conn.peerId, remoteInfo)
      let localPayload = encodeIdentity(proto.registry.localInfo).toBytes()
      await conn.writeLp(localPayload)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debugEcho "dex identity handler error: ", exc.msg
    finally:
      try:
        await conn.closeWithEOF()
      except CatchableError:
        discard
  proto.handler = handle
  proto.codec = DexIdentityCodec

proc new*(T: typedesc[DexIdentityProtocol], registry: DexIdentityRegistry): T =
  let proto = T(registry: registry)
  proto.init()
  proto

proc requestIdentity*(network: DexNetwork, peer: PeerId): Future[DexIdentityInfo] {.async.} =
  try:
    let conn = await network.switch.dial(peer, @[DexIdentityCodec])
    defer: await conn.close()
    let payload = encodeIdentity(network.registry.localInfo).toBytes()
    await conn.writeLp(payload)
    let response = await conn.readLp(64 * 1024)
    var remoteInfo: DexIdentityInfo
    if decodeIdentity(string.fromBytes(response), remoteInfo):
      await network.registry.updateRemote(peer, remoteInfo)
      return remoteInfo
    raise newException(ValueError, "peer returned no identity payload")
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    raise newException(ValueError, "request identity failed: " & exc.msg)

proc allow*(policy: var DexAccessPolicy, topic: string, roles: openArray[DexRole]) =
  var set =
    if policy.hasKey(topic): policy[topic] else: initHashSet[DexRole]()
  for role in roles:
    set.incl(role)
  policy[topic] = set

proc checkAccess*(policy: DexAccessPolicy, info: DexIdentityInfo, topic: string): bool =
  if not policy.hasKey(topic):
    return true
  let required = policy[topic]
  for role in info.roles:
    if required.contains(role):
      return true
  false

proc defaultDexNetworkConfig*(): DexNetworkConfig =
  DexNetworkConfig(
    listenAddrs: @[MultiAddress.init("/ip4/0.0.0.0/tcp/0").tryGet()],
    privateNetworkKey: Opt.none(PrivateNetworkKey),
    enableAutonat: true,
    enableRelay: true,
    metricsPort: Opt.none(int),
    advertisedProtocols: @[],
  )

proc build*(config: DexNetworkConfig, registry: DexIdentityRegistry, policy: DexAccessPolicy = initTable[string, RoleSet]()): DexNetwork =
  var builder = SwitchBuilder
    .new()
    .withRng(newRng())
    .withAddresses(config.listenAddrs)
    .withTcpTransport()
    .withNoise()
    .withYamux()
    .withMplex()
    .withSignedPeerRecord()

  if config.enableAutonat:
    builder = builder.withAutonat().withAutonatV2()

  if config.enableRelay:
    builder = builder.withCircuitRelay()

  config.privateNetworkKey.withValue(key):
    builder = builder.withPnet(key)

  if config.metricsPort.isSome():
    builder = builder.withMetricsExporter(port = Port(config.metricsPort.get().int16))

  let switch =
    try:
      builder.build()
    except CatchableError as exc:
      raise newException(ValueError, "switch build failed: " & exc.msg)

  if config.advertisedProtocols.len > 0:
    for proto in config.advertisedProtocols:
      if proto notin switch.peerInfo.protocols:
        switch.peerInfo.protocols.add(proto)

  let identityProtocol = DexIdentityProtocol.new(registry)
  switch.mount(identityProtocol)

  DexNetwork(
    switch: switch,
    registry: registry,
    identityProtocol: identityProtocol,
    accessPolicy: policy,
  )

proc start*(network: var DexNetwork): Future[void] {.async.} =
  await network.switch.start()

proc stop*(network: var DexNetwork): Future[void] {.async.} =
  await network.switch.stop()

proc publishAllowed*(network: DexNetwork, peer: PeerId, topic: string): Future[bool] {.async.} =
  let remote = await network.registry.getRemote(peer)
  if remote.isNone():
    return false
  let remoteInfo = remote.get()
  network.accessPolicy.checkAccess(remoteInfo, topic)

when isMainModule:
  let localPeerRes = PeerId.random()
  if localPeerRes.isErr():
    raise newException(ValueError, "failed to generate peer id")
  let localPeer = localPeerRes.unsafeValue

  let localIdentity = DexIdentityInfo(
    version: 1'u16,
    peerId: localPeer,
    roles: newRoleSet([drMatcher, drKline]),
    labels: {"env": "dev", "region": "ap-sg-1"}.toTable(),
    publishedAt: getTime(),
  )
  let registry = newDexIdentityRegistry(localIdentity)
  var policy: DexAccessPolicy = initTable[string, RoleSet]()
  policy.allow("dex.orders", [drTrader, drMatcher])
  policy.allow("dex.kline.1m", [drKline, drObserver])

  let config = DexNetworkConfig(
    listenAddrs: @[MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet()],
    enableAutonat: false,
    enableRelay: false,
  )

  let networkRes = config.build(registry, policy)
  if networkRes.isErr():
    raise newException(ValueError, "failed to build dex network")
  let network = networkRes.get()
  echo "dex network prepared: ", network.switch.peerInfo.shortLog()
