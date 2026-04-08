import std/[algorithm, json, options, os, sequtils, sets, strutils, tables, times, unittest]

import chronos

import ../libp2p/lsmr
import ../libp2p/fabric
import ../libp2p/fabric/net/service
import ../libp2p/multiaddress
import ../libp2p/peerstore
import ../libp2p/rwad/identity
import ../libp2p/rwad/types
import ../libp2p/services/lsmrservice

const
  FabricTargetTxCount {.intdefine.} = -1
  FabricProbeAfterSubmitMs {.intdefine.} = -1
  MaxHealthyControlApplyMs = 1500'i64
  MaxHealthySubmitConnectMs = 2000'i64
  MaxHealthySubmitAckMs = 2000'i64
  MaxHealthyInboundSliceMs = 1000'i64
  MaxHealthyMaintenanceSliceMs = 1000'i64

proc makeGenesis(validators: seq[NodeIdentity], observer: NodeIdentity): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = "fabric-network-lsmr"
  result = GenesisSpec(
    params: params,
    validators: @[],
    balances: @[KeyValueU64(key: params.rewardPoolAccount, value: 5_000_000)],
    names: @[],
    contents: @[],
    mintedSupply: 5_000_000,
  )
  for identity in validators:
    result.validators.add(ValidatorInfo(
      address: identity.account,
      peerId: peerIdString(identity.peerId),
      publicKey: encodePublicKeyHex(identity.publicKey),
      stake: 10_000,
      delegatedStake: 0,
      active: true,
      jailed: false,
      slashedAmount: 0,
      lastUpdatedAt: 0,
    ))
    result.balances.add(KeyValueU64(key: identity.account, value: 1_000_000))
  result.balances.add(KeyValueU64(key: observer.account, value: 1_000_000))

proc publishPayload(identity: NodeIdentity, nonce: uint64, suffix: string): JsonNode =
  %*{
    "intent": {
      "contentId": "net-post-" & suffix,
      "author": identity.account,
      "title": "net-post-" & suffix,
      "body": "network body " & suffix,
      "kind": "text",
      "manifestCid": "net-manifest-" & suffix,
      "createdAt": int(nonce),
      "accessPolicy": "public",
      "previewCid": "net-preview-" & suffix,
      "seqNo": int(nonce)
    }
  }

proc waitUntil(
    predicate: proc(): bool {.closure, gcsafe.}, timeoutMs = 30_000, intervalMs = 100
): Future[void] {.async: (raises: [CancelledError, Exception]).} =
  let attempts = max(1, timeoutMs div intervalMs)
  for _ in 0 ..< attempts:
    if predicate():
      return
    await sleepAsync(intervalMs)
  raise newException(ValueError, "condition timed out")

proc activeCertCount(node: FabricNode): int {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.fabricStatus().routingStatus.lsmrActiveCertificates
    except CatchableError:
      return -1

proc contentCount(node: FabricNode): int {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      let projection = node.queryProjection("content")
      if projection.kind == JObject and
          projection.hasKey("contents") and
          projection["contents"].kind == JArray:
        return projection["contents"].len
    except CatchableError:
      discard
    return -1

proc controlSlowApplyCount(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      let svc = getLsmrService(node.network.switch)
      return svc.controlSlowApplyCount()
    except CatchableError:
      return -1

proc controlMaxApplyMs(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      let svc = getLsmrService(node.network.switch)
      return svc.controlMaxApplyElapsedMs()
    except CatchableError:
      return -1

proc submitSlowConnectCount(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.network.submitSlowConnects()
    except CatchableError:
      return -1

proc submitMaxConnectMs(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.network.submitMaxConnectMs()
    except CatchableError:
      return -1

proc submitWriteFailures(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.network.submitWriteFailures()
    except CatchableError:
      return -1

proc submitSlowAckCount(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.network.submitSlowAcks()
    except CatchableError:
      return -1

proc submitMaxAckMs(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.network.submitMaxAckMs()
    except CatchableError:
      return -1

proc inboundSlowSliceCount(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.slowInboundSlices()
    except CatchableError:
      return -1

proc inboundMaxSliceMs(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.maxInboundSliceMs()
    except CatchableError:
      return -1

proc maintenanceSlowSliceCount(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.slowMaintenanceSlices()
    except CatchableError:
      return -1

proc maintenanceMaxSliceMs(node: FabricNode): int64 {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      return node.maxMaintenanceSliceMs()
    except CatchableError:
      return -1

proc submitReadySummary(node: FabricNode): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      let stats = node.network.submitDesiredPeerStats()
      return $stats.ready & "/" & $stats.total & " " & stats.detail
    except CatchableError:
      return "-1/-1"

proc pendingBacklog(node: FabricNode): string {.gcsafe, raises: [].} =
  $(node.pendingEvents.toSeq().len) & "/" & $(node.pendingAttestations.toSeq().len) & "/" &
    $(node.pendingEventCertificates.toSeq().len)

proc overlayPathology(nodes: seq[FabricNode]): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    for idx, node in nodes:
      let slowApply = node.controlSlowApplyCount()
      let maxApplyMs = node.controlMaxApplyMs()
      let slowInbound = node.inboundSlowSliceCount()
      let maxInboundMs = node.inboundMaxSliceMs()
      let slowMaintenance = node.maintenanceSlowSliceCount()
      let maxMaintenanceMs = node.maintenanceMaxSliceMs()
      if maxApplyMs >= MaxHealthyControlApplyMs:
        return "node=" & $idx & " slowControlApply=" & $slowApply &
          " maxApplyMs=" & $maxApplyMs
      if maxInboundMs >= MaxHealthyInboundSliceMs:
        return "node=" & $idx & " slowInbound=" & $slowInbound &
          " maxInboundMs=" & $maxInboundMs
      if maxMaintenanceMs >= MaxHealthyMaintenanceSliceMs:
        return "node=" & $idx & " slowMaintenance=" & $slowMaintenance &
          " maxMaintenanceMs=" & $maxMaintenanceMs
    ""

proc networkPathology(nodes: seq[FabricNode]): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    for idx, node in nodes:
      let submitStats = node.network.submitDesiredPeerStats()
      let submitReadyHealthy = submitStats.total >= 0 and submitStats.ready >= submitStats.total
      let slowApply = node.controlSlowApplyCount()
      let maxApplyMs = node.controlMaxApplyMs()
      let slowConnect = node.submitSlowConnectCount()
      let maxConnectMs = node.submitMaxConnectMs()
      let slowAck = node.submitSlowAckCount()
      let maxAckMs = node.submitMaxAckMs()
      let writeFailures = node.submitWriteFailures()
      let slowInbound = node.inboundSlowSliceCount()
      let maxInboundMs = node.inboundMaxSliceMs()
      let slowMaintenance = node.maintenanceSlowSliceCount()
      let maxMaintenanceMs = node.maintenanceMaxSliceMs()
      if writeFailures > 0 and not submitReadyHealthy:
        return "node=" & $idx & " submitWriteFailures=" & $writeFailures &
          " maxConnectMs=" & $maxConnectMs
      if maxConnectMs >= MaxHealthySubmitConnectMs and not submitReadyHealthy:
        return "node=" & $idx & " slowSubmitConnect=" & $slowConnect &
          " maxConnectMs=" & $maxConnectMs
      if maxAckMs >= MaxHealthySubmitAckMs:
        return "node=" & $idx & " slowSubmitAck=" & $slowAck &
          " maxAckMs=" & $maxAckMs
      if maxApplyMs >= MaxHealthyControlApplyMs:
        return "node=" & $idx & " slowControlApply=" & $slowApply &
          " maxApplyMs=" & $maxApplyMs
      if maxInboundMs >= MaxHealthyInboundSliceMs:
        return "node=" & $idx & " slowInbound=" & $slowInbound &
          " maxInboundMs=" & $maxInboundMs
      if maxMaintenanceMs >= MaxHealthyMaintenanceSliceMs:
        return "node=" & $idx & " slowMaintenance=" & $slowMaintenance &
          " maxMaintenanceMs=" & $maxMaintenanceMs
    ""

proc progressFingerprint(nodes: seq[FabricNode]): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    for idx, node in nodes:
      var snapshot = $idx & ":-1/-1/-1/-1/-1/0/-1/-1/-1/-1/-1/-1/-1"
      try:
        let status = node.fabricStatus()
        let checkpoint = node.getCheckpoint()
        snapshot =
          $idx & ":" &
          $status.eventCount & "/" &
          $status.certifiedEventCount & "/" &
          $status.checkpointCount & "/" &
          $status.routingStatus.lsmrActiveCertificates & "/" &
          $node.contentCount() & "/" &
          $(if checkpoint.isSome(): 1 else: 0) & "/" &
          node.pendingBacklog() & "/" &
          $node.controlMaxApplyMs() & "/" &
          $node.submitMaxConnectMs() & "/" &
          $node.submitMaxAckMs() & "/" &
          $node.submitWriteFailures() & "/" &
          $node.maxInboundSliceMs() & "/" &
          $node.maxMaintenanceSliceMs()
      except CatchableError:
        discard
      if result.len > 0:
        result.add(" | ")
      result.add(snapshot)

proc checkpointReady(nodes: seq[FabricNode], targetTxCount: int): bool {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    try:
      for node in nodes:
        if node.getCheckpoint().isNone():
          return false
      let projection = nodes[^1].queryProjection("content")
      projection.kind == JObject and
        projection.hasKey("contents") and
        projection["contents"].kind == JArray and
        projection["contents"].len == targetTxCount
    except CatchableError:
      false

proc dumpNodeState(nodes: seq[FabricNode]) {.gcsafe, raises: [].}

proc awaitOverlayReady(
    nodes: seq[FabricNode], timeoutMs: int, stallMs = 3000, intervalMs = 100
) {.async: (raises: [CancelledError, Exception]).} =
  let startedAtMs = int64(epochTime() * 1000)
  var lastProgressAtMs = startedAtMs
  var lastFingerprint = ""
  while true:
    let fingerprint = nodes.progressFingerprint()
    let pathology = nodes.overlayPathology()
    if fingerprint != lastFingerprint:
      echo "OVERLAY-PROGRESS ", fingerprint
      lastFingerprint = fingerprint
      lastProgressAtMs = int64(epochTime() * 1000)
    if pathology.len > 0:
      dumpNodeState(nodes)
      raise newException(ValueError, "overlay unhealthy " & pathology & " fingerprint=" & fingerprint)
    var ready = true
    for node in nodes:
      if node.activeCertCount() < nodes.len:
        ready = false
        break
    if ready:
      echo "OVERLAY-READY ", fingerprint
      return
    let nowMs = int64(epochTime() * 1000)
    if timeoutMs >= 0 and nowMs - startedAtMs >= timeoutMs.int64:
      dumpNodeState(nodes)
      raise newException(ValueError, "overlay timed out fingerprint=" & fingerprint)
    if stallMs >= 0 and nowMs - lastProgressAtMs >= stallMs.int64:
      dumpNodeState(nodes)
      raise newException(ValueError, "overlay stalled fingerprint=" & fingerprint)
    await sleepAsync(intervalMs)

proc awaitSubmitReady(
    nodes: seq[FabricNode], timeoutMs: int, stallMs = 3000, intervalMs = 100
) {.async: (raises: [CancelledError, Exception]).} =
  let startedAtMs = int64(epochTime() * 1000)
  var lastProgressAtMs = startedAtMs
  var lastFingerprint = ""
  while true:
    var fingerprintParts: seq[string] = @[]
    var ready = true
    for idx, node in nodes:
      let summary = node.submitReadySummary()
      fingerprintParts.add($idx & ":" & summary)
      try:
        let stats = node.network.submitDesiredPeerStats()
        if stats.ready < stats.total:
          ready = false
      except CatchableError:
        ready = false
    let fingerprint = fingerprintParts.join(" | ")
    if fingerprint != lastFingerprint:
      echo "SUBMIT-READY-PROGRESS ", fingerprint
      lastFingerprint = fingerprint
      lastProgressAtMs = int64(epochTime() * 1000)
    if ready:
      echo "SUBMIT-READY ", fingerprint
      return
    let nowMs = int64(epochTime() * 1000)
    if timeoutMs >= 0 and nowMs - startedAtMs >= timeoutMs.int64:
      dumpNodeState(nodes)
      raise newException(ValueError, "submit ready timed out fingerprint=" & fingerprint)
    if stallMs >= 0 and nowMs - lastProgressAtMs >= stallMs.int64:
      dumpNodeState(nodes)
      raise newException(ValueError, "submit ready stalled fingerprint=" & fingerprint)
    await sleepAsync(intervalMs)

proc awaitCheckpointConvergence(
    nodes: seq[FabricNode], targetTxCount: int, timeoutMs: int, stallMs = 10000, intervalMs = 250
) {.async: (raises: [CancelledError, Exception]).} =
  let startedAtMs = int64(epochTime() * 1000)
  var lastProgressAtMs = startedAtMs
  var lastFingerprint = ""
  while true:
    let fingerprint = nodes.progressFingerprint()
    let pathology = nodes.networkPathology()
    if fingerprint != lastFingerprint:
      echo "CHECKPOINT-PROGRESS ", fingerprint
      lastFingerprint = fingerprint
      lastProgressAtMs = int64(epochTime() * 1000)
    if pathology.len > 0:
      dumpNodeState(nodes)
      raise newException(ValueError, "checkpoint unhealthy " & pathology & " fingerprint=" & fingerprint)
    if nodes.checkpointReady(targetTxCount):
      echo "CHECKPOINT-READY ", fingerprint
      return
    let nowMs = int64(epochTime() * 1000)
    if timeoutMs >= 0 and nowMs - startedAtMs >= timeoutMs.int64:
      dumpNodeState(nodes)
      raise newException(ValueError, "checkpoint timed out fingerprint=" & fingerprint)
    if stallMs >= 0 and nowMs - lastProgressAtMs >= stallMs.int64:
      dumpNodeState(nodes)
      raise newException(ValueError, "checkpoint stalled fingerprint=" & fingerprint)
    await sleepAsync(intervalMs)

proc assertQuiesced(node: FabricNode, expectedCommittedNonce: uint64) =
  let snapshot = node.submitFence()
  check snapshot.pendingInbound == 0
  check snapshot.localOfferedNonce == expectedCommittedNonce
  check snapshot.localCommittedNonce == expectedCommittedNonce
  check snapshot.pendingEvents == 0
  check snapshot.pendingAttestations == 0
  check snapshot.pendingEventCertificates == 0
  check snapshot.pendingCertificationEvents == 0
  check snapshot.pendingWitnessPulls == 0
  check snapshot.pendingIndexFlushes == 0
  check snapshot.eventOutstanding == 0
  check snapshot.attOutstanding == 0
  check snapshot.certOutstanding == 0

proc stopAll(nodes: seq[FabricNode]) {.async.} =
  for node in nodes:
    try:
      await node.prepareStop()
    except CatchableError:
      discard
  for idx in countdown(nodes.len - 1, 0):
    try:
      await nodes[idx].stop()
    except CatchableError:
      discard

proc rawAddrs(node: FabricNode): seq[string] =
  if node.isNil or node.network.isNil or node.network.switch.isNil:
    return @[]
  for addr in node.network.switch.peerInfo.addrs:
    result.add($addr)

proc parseAddrs(values: seq[string]): seq[MultiAddress] =
  for value in values:
    let parsed = MultiAddress.init(value)
    if parsed.isOk():
      result.add(parsed.get())

proc rootAnchor(identity: NodeIdentity, operatorId: string, addrs: seq[string] = @[]): LsmrAnchor =
  LsmrAnchor(
    peerId: identity.peerId,
    addrs: parseAddrs(addrs),
    operatorId: operatorId,
    regionDigit: 5'u8,
    attestedPrefix: @[5'u8],
    serveDepth: 2'u8,
    directionMask: 0x1ff'u32,
    canIssueRootCert: true,
  )

proc makeLsmrConfig(
    networkId, operatorId: string,
    localSuffix: LsmrPath,
    anchors: seq[LsmrAnchor],
): LsmrConfig =
  LsmrConfig.init(
    networkId = networkId,
    anchors = anchors,
    serveWitness = true,
    operatorId = operatorId,
    regionDigit = 5'u8,
    localSuffix = localSuffix,
    serveDepth = 2,
    minWitnessQuorum = 1,
  )

proc makeNodeConfig(
    baseDir: string,
    identity: NodeIdentity,
    genesis: GenesisSpec,
    localSuffix: LsmrPath,
    bootstrapAddrs: seq[string],
    anchors: seq[LsmrAnchor],
): FabricNodeConfig =
  createDir(baseDir)
  saveIdentity(baseDir / "identity.json", identity)
  saveGenesis(baseDir / "genesis.json", genesis)
  FabricNodeConfig(
    dataDir: baseDir,
    identityPath: baseDir / "identity.json",
    genesisPath: baseDir / "genesis.json",
    listenAddrs: @["/ip4/127.0.0.1/tcp/0"],
    bootstrapAddrs: @[],
    legacyBootstrapAddrs: @[],
    lsmrBootstrapAddrs: bootstrapAddrs,
    lsmrPath: @[5'u8] & localSuffix,
    routingMode: RoutingPlaneMode.lsmrOnly,
    primaryPlane: PrimaryRoutingPlane.lsmr,
    lsmrConfig: some(makeLsmrConfig(genesis.params.chainId, identity.account, localSuffix, anchors)),
  )

proc dumpNodeState(nodes: seq[FabricNode]) {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    for idx, node in nodes:
      var account = ""
      var eventCount = -1
      var certifiedEventCount = -1
      var checkpointCount = -1
      var latestEra = uint64(0)
      var activeCerts = -1
      var hasCheckpoint = false
      var projectionCount = -1
      var backlog = "-1/-1/-1"
      var slowApply = int64(-1)
      var maxApplyMs = int64(-1)
      var slowConnect = int64(-1)
      var maxConnectMs = int64(-1)
      var writeFailures = int64(-1)
      var submitReady = "-1/-1"
      try:
        let status = node.fabricStatus()
        let checkpoint = node.getCheckpoint()
        account = status.localAccount
        eventCount = status.eventCount
        certifiedEventCount = status.certifiedEventCount
        checkpointCount = status.checkpointCount
        latestEra = status.latestCheckpointEra
        activeCerts = status.routingStatus.lsmrActiveCertificates
        hasCheckpoint = checkpoint.isSome()
        projectionCount = node.contentCount()
        backlog = node.pendingBacklog()
        slowApply = node.controlSlowApplyCount()
        maxApplyMs = node.controlMaxApplyMs()
        slowConnect = node.submitSlowConnectCount()
        maxConnectMs = node.submitMaxConnectMs()
        writeFailures = node.submitWriteFailures()
        submitReady = node.submitReadySummary()
      except CatchableError:
        discard
      echo "FABRIC_LSMR node=", idx,
        " account=", account,
        " events=", eventCount,
        " certs=", certifiedEventCount,
        " checkpoints=", checkpointCount,
        " latestEra=", latestEra,
        " activeCerts=", activeCerts,
        " checkpoint=", hasCheckpoint,
        " contentCount=", projectionCount,
        " backlog=", backlog,
        " slowApply=", slowApply,
        " maxApplyMs=", maxApplyMs,
        " slowConnect=", slowConnect,
        " maxConnectMs=", maxConnectMs,
        " writeFailures=", writeFailures,
        " submitReady=", submitReady
      try:
        var activePrefixes: seq[string] = @[]
        if not node.network.isNil and not node.network.switch.isNil and
            not node.network.switch.peerStore.isNil:
          for peerId, record in node.network.switch.peerStore[ActiveLsmrBook].book.pairs:
            activePrefixes.add($peerId & "=" & $record.data.certifiedPrefix())
        activePrefixes.sort(system.cmp[string])
        echo "FABRIC_LSMR_ACTIVE node=", idx,
          " prefixes=", activePrefixes.join(",")
      except CatchableError:
        discard
      try:
        var routingTable: seq[string] = @[]
        for account, route in node.routingPeers.pairs:
          routingTable.add(account & "=" & route.peerId & "@" & $route.path)
        routingTable.sort(system.cmp[string])
        echo "FABRIC_LSMR_ROUTES node=", idx,
          " routes=", routingTable.join(",")
      except CatchableError:
        discard

suite "Fabric lsmr only network":
  test "real network converges without kad or rendezvous":
    let baseDir = getTempDir() / ("fabric-network-lsmr-only-" & $getTime().toUnix())
    createDir(baseDir)
    let v0 = newNodeIdentity()
    let v1 = newNodeIdentity()
    let v2 = newNodeIdentity()
    let observerId = newNodeIdentity()
    let genesis = makeGenesis(@[v0, v1, v2], observerId)
    var nodes: seq[FabricNode] = @[]
    try:
      let n0 = newFabricNode(makeNodeConfig(
        baseDir / "node-0",
        v0,
        genesis,
        @[1'u8],
        @[],
        @[rootAnchor(v0, "root-a"), rootAnchor(v1, "root-b")],
      ))
      nodes.add(n0)
      waitFor n0.start()

      let anchor0Addrs = n0.rawAddrs()
      let n1 = newFabricNode(makeNodeConfig(
        baseDir / "node-1",
        v1,
        genesis,
        @[9'u8],
        anchor0Addrs,
        @[rootAnchor(v0, "root-a", anchor0Addrs), rootAnchor(v1, "root-b")],
      ))
      nodes.add(n1)
      waitFor n1.start()

      let anchor1Addrs = n1.rawAddrs()
      let anchors = @[
        rootAnchor(v0, "root-a", anchor0Addrs),
        rootAnchor(v1, "root-b", anchor1Addrs),
      ]
      let bootstrap = anchor0Addrs & anchor1Addrs
      let n2 = newFabricNode(makeNodeConfig(baseDir / "node-2", v2, genesis, @[3'u8], bootstrap, anchors))
      nodes.add(n2)
      waitFor n2.start()

      let n3 = newFabricNode(makeNodeConfig(baseDir / "node-3", observerId, genesis, @[7'u8], bootstrap, anchors))
      nodes.add(n3)
      waitFor n3.start()

      for node in nodes:
        check not node.network.isNil
        check node.network.kad.isNil
        check node.network.rendezvous.isNil

      try:
        waitFor awaitOverlayReady(nodes, timeoutMs = 10_000, stallMs = 3_000, intervalMs = 100)
      except CatchableError:
        dumpNodeState(nodes)
        raise

      for node in nodes:
        node.enableLsmrDataPlane()

      try:
        waitFor awaitSubmitReady(nodes, timeoutMs = 10_000, stallMs = 3_000, intervalMs = 100)
      except CatchableError:
        dumpNodeState(nodes)
        raise

      for node in nodes:
        node.network.resetSubmitMetrics()

      when defined(fabric_diag):
        for idx, node in nodes:
          let status = node.fabricStatus()
          echo "routing node=", idx,
            " account=", status.localAccount,
            " activeCerts=", status.routingStatus.lsmrActiveCertificates

      let targetTxCount =
        when defined(fabric_diag):
          4
        elif FabricTargetTxCount > 0:
          FabricTargetTxCount
        else:
          61
      for idx in 0 ..< targetTxCount:
        let submitStartedAtMs = int64(epochTime() * 1000)
        echo "SUBMIT-START idx=", idx
        let nextNonce = n0.nextLocalNonce()
        let submit = n0.submitLocalTx(
          txContentPublishIntent,
          publishPayload(v0, nextNonce, "lsmr-v0-" & $idx),
          timestamp = int64(nextNonce),
        )
        let submitElapsedMs = int64(epochTime() * 1000) - submitStartedAtMs
        echo "SUBMIT-DONE idx=", idx,
          " elapsedMs=", submitElapsedMs,
          " accepted=", submit.accepted,
          " certified=", submit.certified
        when defined(fabric_diag):
          echo "submit#", idx, " accepted=", submit.accepted, " certified=", submit.certified
          let event = n0.getEvent(submit.eventId)
          if event.isSome():
            echo "event#", idx,
              " clock=", event.get().clock,
              " witnessAccounts=", event.get().witnessSet,
              " witnessRoutes=", event.get().witnessRoutes,
              " participantRoutes=", event.get().participantRoutes
        check submit.accepted

      when FabricProbeAfterSubmitMs > 0:
        waitFor sleepAsync(FabricProbeAfterSubmitMs)
        dumpNodeState(nodes)
      else:
        try:
          waitFor awaitCheckpointConvergence(
            nodes,
            targetTxCount,
            timeoutMs = 90_000,
            stallMs = 10_000,
            intervalMs = 250,
          )
        except CatchableError:
          dumpNodeState(nodes)
          raise

        let checkpoint0 = nodes[0].getCheckpoint()
        let checkpoint1 = nodes[1].getCheckpoint()
        let checkpoint2 = nodes[2].getCheckpoint()
        let checkpoint3 = nodes[3].getCheckpoint()
        check checkpoint0.isSome()
        check checkpoint1.isSome()
        check checkpoint2.isSome()
        check checkpoint3.isSome()
        check checkpoint0.get().checkpointId == checkpoint1.get().checkpointId
        check checkpoint0.get().checkpointId == checkpoint2.get().checkpointId
        check checkpoint0.get().checkpointId == checkpoint3.get().checkpointId

        for idx, node in nodes:
          let expectedNonce =
            if idx == 0: uint64(targetTxCount)
            else: 0'u64
          node.assertQuiesced(expectedNonce)

        let observerProjection = nodes[^1].queryProjection("content")
        check observerProjection["contents"].len == targetTxCount
        let observerDetail = nodes[^1].contentDetail("net-post-lsmr-v0-0")
        check observerDetail.isSome()
        check observerDetail.get().title == "net-post-lsmr-v0-0"
        check nodes[^1].feedSnapshot(10).len == 10
    finally:
      if nodes.len > 0:
        waitFor stopAll(nodes)
