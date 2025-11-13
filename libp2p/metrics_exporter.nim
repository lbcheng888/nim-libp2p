# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[sets, tables, options, times]
import chronos, metrics, chronicles

import ./switch
when not defined(libp2p_disable_datatransfer):
  import ./protocols/datatransfer/channelmanager
import ./resourcemanager
import ./bandwidthmanager
import ./protocols/kademlia/kademlia
import ./protocols/graphsync/graphsync
import ./protocols/bitswap/ledger
import ./providers/bitswapadvertiser
import ./utility
when defined(libp2p_msquic_experimental):
  import ./transports/msquictransport as quictransport
elif defined(libp2p_quic_support):
  import ./transports/quictransport

const
  BitswapAdvertTargets = ["dht", "delegated"]
  BitswapAdvertResults = ["success", "failure"]
  BitswapAdvertSkipTarget = "all"

proc momentToUnixSeconds(moment: Moment): float {.inline.} =
  float(moment.epochNanoSeconds()) / 1_000_000_000.0

declarePublicGauge(
  libp2p_resource_connections,
  "Resource manager active connections",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_resource_streams,
  "Resource manager active streams",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_resource_streams_protocol,
  "Resource manager streams per protocol",
  labels = ["protocol", "direction"],
)
declarePublicGauge(
  libp2p_resource_shared_pool_connections,
  "Resource manager shared pool connections",
  labels = ["pool", "direction"],
)
declarePublicGauge(
  libp2p_resource_shared_pool_streams,
  "Resource manager shared pool streams",
  labels = ["pool", "direction"],
)

declarePublicGauge(
  libp2p_bandwidth_total_bytes,
  "Aggregated bandwidth totals (bytes)",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bandwidth_rate_bps,
  "Aggregated bandwidth rate (bytes per second)",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bandwidth_protocol_bytes,
  "Bandwidth per protocol (bytes)",
  labels = ["protocol", "direction"],
)
declarePublicGauge(
  libp2p_bandwidth_protocol_rate_bps,
  "Bandwidth per protocol rate (bytes per second)",
  labels = ["protocol", "direction"],
)
declarePublicGauge(
  libp2p_bandwidth_group_bytes,
  "Bandwidth per protocol group (bytes)",
  labels = ["group", "direction"],
)
declarePublicGauge(
  libp2p_bandwidth_group_rate_bps,
  "Bandwidth per protocol group rate (bytes per second)",
  labels = ["group", "direction"],
)

declarePublicGauge(
  libp2p_bitswap_bytes_total,
  "Bitswap ledger byte totals",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bitswap_blocks_total,
  "Bitswap ledger block totals",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bitswap_have_total,
  "Bitswap HAVE signals tracked in ledger",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bitswap_dont_have_total,
  "Bitswap DONT_HAVE signals tracked in ledger",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bitswap_wants_total,
  "Bitswap wants tracked in ledger",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_bitswap_exchanges_total,
  "Bitswap exchanges recorded by ledger",
)
declarePublicGauge(
  libp2p_bitswap_peers,
  "Bitswap peers tracked in ledger",
)
declarePublicGauge(
  libp2p_bitswap_max_debt_ratio,
  "Maximum Bitswap debt ratio across peers",
)
declarePublicGauge(
  libp2p_bitswap_advertisements_total,
  "Bitswap provider advertisements",
  labels = ["target", "result"],
)
declarePublicGauge(
  libp2p_bitswap_advertisement_last_success_timestamp,
  "Unix timestamp of last successful Bitswap advertisement",
  labels = ["target"],
)
declarePublicGauge(
  libp2p_bitswap_advertisement_last_attempt_timestamp,
  "Unix timestamp of last Bitswap advertisement attempt",
)
declarePublicGauge(
  libp2p_bitswap_top_debtor_ratio,
  "Bitswap debt ratio for the top debtor peers",
  labels = ["peer", "rank"],
)
declarePublicGauge(
  libp2p_bitswap_top_debtor_bytes,
  "Bitswap bytes exchanged with the top debtor peers",
  labels = ["peer", "rank", "direction"],
)

declarePublicGauge(
  libp2p_graphsync_requests_total,
  "GraphSync requests handled",
  labels = ["direction", "outcome"],
)
declarePublicGauge(
  libp2p_graphsync_blocks_total,
  "GraphSync blocks transferred",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_graphsync_bytes_total,
  "GraphSync bytes transferred",
  labels = ["direction"],
)
declarePublicGauge(
  libp2p_graphsync_missing_links_total,
  "GraphSync links reported as missing",
  labels = ["direction"],
)

when not defined(libp2p_disable_datatransfer):
  declarePublicGauge(
    libp2p_datatransfer_channels_total,
    "Data transfer channels tracked by the manager",
  )
  declarePublicGauge(
    libp2p_datatransfer_channels,
    "Data transfer channels by status",
    labels = ["direction", "status"],
  )
  declarePublicGauge(
    libp2p_datatransfer_paused,
    "Data transfer channels paused state",
    labels = ["direction", "kind"],
  )

declarePublicGauge(
  libp2p_ipns_records_total,
  "IPNS records tracked by republisher",
  labels = ["state"],
)
declarePublicGauge(
  libp2p_ipns_namespace_records,
  "IPNS records per namespace",
  labels = ["namespace", "state"],
)
declarePublicGauge(
  libp2p_ipns_refresh_total,
  "IPNS refresh outcomes per namespace",
  labels = ["namespace", "result"],
)
declarePublicGauge(
  libp2p_ipns_next_attempt_seconds,
  "Seconds until the earliest scheduled IPNS refresh per namespace",
  labels = ["namespace"],
)
declarePublicGauge(
  libp2p_ipns_last_event_unix,
  "Unix timestamp of the last IPNS refresh event",
  labels = ["namespace", "event"],
)

when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
  type WebtransportMetricsState* = ref object
    certLabels*: HashSet[string]

  declarePublicGauge(
    libp2p_webtransport_cert_hash,
    "WebTransport certificate hashes exposed to clients",
    labels = ["state", "hash"],
  )
  declarePublicGauge(
    libp2p_webtransport_max_sessions,
    "Configured WebTransport session concurrency limit",
  )
  declarePublicGauge(
    libp2p_webtransport_rotation_interval_seconds,
    "Configured WebTransport certificate rotation interval (seconds)",
  )
  declarePublicGauge(
    libp2p_webtransport_rotation_keep_history,
    "Configured WebTransport certificate rotation history size",
  )
  declarePublicGauge(
    libp2p_webtransport_last_rotation_timestamp,
    "Unix timestamp of the last successful WebTransport certificate rotation",
  )
  declarePublicGauge(
    libp2p_webtransport_sessions,
    "Active WebTransport sessions grouped by readiness state",
    labels = ["state"],
  )
  declarePublicGauge(
    libp2p_webtransport_handshake_seconds,
    "Average WebTransport handshake durations grouped by state (seconds)",
    labels = ["state"],
  )
  declarePublicGauge(
    libp2p_webtransport_rejections_total,
    "WebTransport handshake rejections grouped by reason",
    labels = ["reason"],
  )

  proc initWebtransportMetricsState*(): WebtransportMetricsState {.inline.} =
    new result
    result.certLabels = initHashSet[string]()

type
  IpnsTrackerProc* = proc(): seq[KadDHT] {.gcsafe, raises: [].}
  BitswapMetricsState* = ref object
    topDebtorRatios*: HashSet[string]
    topDebtorBytes*: HashSet[string]
  IpnsMetricsState* = ref object
    namespaces*: HashSet[string]
  SwitchMetricsState* = ref object
    when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
      webtransport*: WebtransportMetricsState
    resourceProtocols*: HashSet[string]
    resourcePools*: HashSet[string]
    bandwidthProtocols*: HashSet[string]
    bandwidthGroups*: HashSet[string]

proc initBitswapMetricsState*(): BitswapMetricsState {.inline.} =
  new result
  result.topDebtorRatios = initHashSet[string]()
  result.topDebtorBytes = initHashSet[string]()

proc initIpnsMetricsState*(): IpnsMetricsState {.inline.} =
  new result
  result.namespaces = initHashSet[string]()

when not defined(libp2p_disable_datatransfer):
  func dataTransferDirectionLabel(dir: DataTransferChannelDirection): string {.inline.} =
    case dir
    of DataTransferChannelDirection.dtcdInbound:
      "inbound"
    of DataTransferChannelDirection.dtcdOutbound:
      "outbound"

  func dataTransferStatusLabel(status: DataTransferChannelStatus): string {.inline.} =
    case status
    of DataTransferChannelStatus.dtcsRequested:
      "requested"
    of DataTransferChannelStatus.dtcsAccepted:
      "accepted"
    of DataTransferChannelStatus.dtcsOngoing:
      "ongoing"
    of DataTransferChannelStatus.dtcsPaused:
      "paused"
    of DataTransferChannelStatus.dtcsCompleted:
      "completed"
    of DataTransferChannelStatus.dtcsCancelled:
      "cancelled"
    of DataTransferChannelStatus.dtcsFailed:
      "failed"

  proc resetDataTransferMetrics() {.gcsafe.} =
    libp2p_datatransfer_channels_total.set(0)
    for dir in DataTransferChannelDirection:
      let dirLabel = dataTransferDirectionLabel(dir)
      for status in DataTransferChannelStatus:
        libp2p_datatransfer_channels.set(
          0,
          labelValues = [dirLabel, dataTransferStatusLabel(status)],
        )
      libp2p_datatransfer_paused.set(
        0, labelValues = [dirLabel, "local"]
      )
      libp2p_datatransfer_paused.set(
        0, labelValues = [dirLabel, "remote"]
      )

  proc updateDataTransferMetrics*(s: Switch) {.public, async.} =
    let statsOpt = await s.dataTransferStats()
    if statsOpt.isNone:
      resetDataTransferMetrics()
      return

    let stats = statsOpt.get()
    libp2p_datatransfer_channels_total.set(float(stats.total))
    for dir in DataTransferChannelDirection:
      let dirLabel = dataTransferDirectionLabel(dir)
      for status in DataTransferChannelStatus:
        let value = stats.counts[dir][status]
        libp2p_datatransfer_channels.set(
          float(value),
          labelValues = [dirLabel, dataTransferStatusLabel(status)],
        )
      libp2p_datatransfer_paused.set(
        float(stats.pausedLocal[dir]), labelValues = [dirLabel, "local"]
      )
      libp2p_datatransfer_paused.set(
        float(stats.pausedRemote[dir]), labelValues = [dirLabel, "remote"]
      )

proc resetMissingResourceProtocols(
    seen: HashSet[string], cache: var HashSet[string]
) =
  for proto in cache:
    if proto notin seen:
      libp2p_resource_streams_protocol.set(0, labelValues = [proto, "inbound"])
      libp2p_resource_streams_protocol.set(0, labelValues = [proto, "outbound"])
  cache.clear()
  for proto in seen:
    cache.incl(proto)

proc resetMissingResourcePools(
    seen: HashSet[string], cache: var HashSet[string]
) =
  for pool in cache:
    if pool notin seen:
      libp2p_resource_shared_pool_connections.set(0, labelValues = [pool, "inbound"])
      libp2p_resource_shared_pool_connections.set(0, labelValues = [pool, "outbound"])
      libp2p_resource_shared_pool_streams.set(0, labelValues = [pool, "inbound"])
      libp2p_resource_shared_pool_streams.set(0, labelValues = [pool, "outbound"])
  cache.clear()
  for pool in seen:
    cache.incl(pool)

proc resetMissingBandwidthProtocols(
    seen: HashSet[string], cache: var HashSet[string]
) =
  for proto in cache:
    if proto notin seen:
      libp2p_bandwidth_protocol_bytes.set(0.0, labelValues = [proto, "inbound"])
      libp2p_bandwidth_protocol_bytes.set(0.0, labelValues = [proto, "outbound"])
      libp2p_bandwidth_protocol_rate_bps.set(0.0, labelValues = [proto, "inbound"])
      libp2p_bandwidth_protocol_rate_bps.set(0.0, labelValues = [proto, "outbound"])
  cache.clear()
  for proto in seen:
    cache.incl(proto)

proc resetMissingBandwidthGroups(
    seen: HashSet[string], cache: var HashSet[string]
) =
  for group in cache:
    if group notin seen:
      libp2p_bandwidth_group_bytes.set(0.0, labelValues = [group, "inbound"])
      libp2p_bandwidth_group_bytes.set(0.0, labelValues = [group, "outbound"])
      libp2p_bandwidth_group_rate_bps.set(0.0, labelValues = [group, "inbound"])
      libp2p_bandwidth_group_rate_bps.set(0.0, labelValues = [group, "outbound"])
  cache.clear()
  for group in seen:
    cache.incl(group)

proc resetBitswapAdvertiserMetrics() =
  for target in BitswapAdvertTargets:
    for result in BitswapAdvertResults:
      libp2p_bitswap_advertisements_total.set(0.0, labelValues = [target, result])
    libp2p_bitswap_advertisement_last_success_timestamp.set(
      0.0, labelValues = [target]
    )
  libp2p_bitswap_advertisements_total.set(
    0.0, labelValues = [BitswapAdvertSkipTarget, "skipped"]
  )
  libp2p_bitswap_advertisement_last_attempt_timestamp.set(0.0)

proc resetMissingBitswapTopDebtors(
    ratioSeen: HashSet[string],
    bytesSeen: HashSet[string],
    state: BitswapMetricsState
) =
  if state.isNil:
    return
  for label in state.topDebtorRatios:
    if label notin ratioSeen:
      var sep = -1
      for idx in 0 ..< label.len:
        if label[idx] == '|':
          sep = idx
          break
      if sep <= 0 or sep >= label.len - 1:
        continue
      let peer = label[0 ..< sep]
      let rank = label[sep + 1 .. ^1]
      libp2p_bitswap_top_debtor_ratio.set(0.0, labelValues = [peer, rank])
  state.topDebtorRatios.clear()
  for label in ratioSeen:
    state.topDebtorRatios.incl(label)

  for label in state.topDebtorBytes:
    if label notin bytesSeen:
      var firstSep = -1
      var secondSep = -1
      for idx in 0 ..< label.len:
        if label[idx] == '|':
          if firstSep < 0:
            firstSep = idx
          elif secondSep < 0:
            secondSep = idx
            break
      if firstSep <= 0 or secondSep <= firstSep + 1 or secondSep >= label.len - 1:
        continue
      let peer = label[0 ..< firstSep]
      let rank = label[firstSep + 1 ..< secondSep]
      let direction = label[secondSep + 1 .. ^1]
      libp2p_bitswap_top_debtor_bytes.set(
        0.0, labelValues = [peer, rank, direction]
      )
  state.topDebtorBytes.clear()
  for label in bytesSeen:
    state.topDebtorBytes.incl(label)

proc resetBitswapMetrics(state: BitswapMetricsState) =
  if state.isNil:
    return
  for direction in ["inbound", "outbound"]:
    libp2p_bitswap_bytes_total.set(0.0, labelValues = [direction])
    libp2p_bitswap_blocks_total.set(0.0, labelValues = [direction])
    libp2p_bitswap_have_total.set(0.0, labelValues = [direction])
    libp2p_bitswap_dont_have_total.set(0.0, labelValues = [direction])
    libp2p_bitswap_wants_total.set(0.0, labelValues = [direction])
  libp2p_bitswap_exchanges_total.set(0.0)
  libp2p_bitswap_peers.set(0.0)
  libp2p_bitswap_max_debt_ratio.set(0.0)
  resetMissingBitswapTopDebtors(
    initHashSet[string](), initHashSet[string](), state
  )
  resetBitswapAdvertiserMetrics()

proc resetGraphSyncMetrics() =
  for direction in ["inbound", "outbound"]:
    for outcome in ["total", "completed", "partial", "failed"]:
      libp2p_graphsync_requests_total.set(
        0.0, labelValues = [direction, outcome]
      )
    libp2p_graphsync_blocks_total.set(0.0, labelValues = [direction])
    libp2p_graphsync_bytes_total.set(0.0, labelValues = [direction])
    libp2p_graphsync_missing_links_total.set(0.0, labelValues = [direction])

proc ipnsLabel(namespace: string): string {.inline.} =
  if namespace.len == 0:
    "(root)"
  else:
    namespace

proc resetMissingIpnsNamespaces(
    seen: HashSet[string], state: IpnsMetricsState
) =
  if state.isNil:
    return
  for namespace in state.namespaces:
    if namespace notin seen:
      let label = ipnsLabel(namespace)
      libp2p_ipns_namespace_records.set(0.0, labelValues = [label, "active"])
      libp2p_ipns_namespace_records.set(0.0, labelValues = [label, "backoff"])
      libp2p_ipns_refresh_total.set(0.0, labelValues = [label, "success"])
      libp2p_ipns_refresh_total.set(0.0, labelValues = [label, "failure"])
      libp2p_ipns_next_attempt_seconds.set(0.0, labelValues = [label])
      libp2p_ipns_last_event_unix.set(0.0, labelValues = [label, "success"])
      libp2p_ipns_last_event_unix.set(0.0, labelValues = [label, "failure"])
  state.namespaces = seen

proc resetIpnsMetrics(state: IpnsMetricsState) =
  if state.isNil:
    return
  libp2p_ipns_records_total.set(0.0, labelValues = ["tracked"])
  libp2p_ipns_records_total.set(0.0, labelValues = ["overdue"])
  resetMissingIpnsNamespaces(initHashSet[string](), state)

when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
  proc resetMissingWebtransportCerts(
      seen: HashSet[string], cache: WebtransportMetricsState
  ) =
    if cache.isNil:
      return
    for label in cache.certLabels:
      if label notin seen:
        var sep = -1
        for idx in 0 ..< label.len:
          if label[idx] == '|':
            sep = idx
            break
        if sep < 0:
          continue
        let state = label[0 ..< sep]
        let hash =
          if sep + 1 < label.len:
            label[sep + 1 .. ^1]
          else:
            ""
        libp2p_webtransport_cert_hash.set(0.0, labelValues = [state, hash])
    cache.certLabels = seen

  proc updateWebtransportMetrics*(
      s: Switch, cache: var WebtransportMetricsState
  ) {.public.} =
    var seen = initHashSet[string]()
    libp2p_webtransport_max_sessions.set(0.0)
    libp2p_webtransport_rotation_interval_seconds.set(0.0)
    libp2p_webtransport_rotation_keep_history.set(0.0)
    libp2p_webtransport_last_rotation_timestamp.set(0.0)
    var readySessions = 0
    var pendingSessions = 0
    libp2p_webtransport_handshake_seconds.set(0.0, labelValues = ["ready"])
    libp2p_webtransport_handshake_seconds.set(0.0, labelValues = ["pending"])
    if s.isNil:
      libp2p_webtransport_sessions.set(0.0, labelValues = ["ready"])
      libp2p_webtransport_sessions.set(0.0, labelValues = ["pending"])
      for reason in quictransport.WebtransportRejectionReason:
        let label = quictransport.webtransportRejectionReasonLabel(reason)
        libp2p_webtransport_rejections_total.set(0.0, labelValues = [label])
      resetMissingWebtransportCerts(seen, cache)
      return

    let currentOpt = s.webtransportCerthash()
    var currentHash = ""
    if currentOpt.isSome:
      currentHash = currentOpt.get()
      if currentHash.len > 0:
        let label = "current|" & currentHash
        seen.incl(label)
        libp2p_webtransport_cert_hash.set(
          1.0, labelValues = ["current", currentHash]
        )

    for hash in s.webtransportCerthashHistory():
      if hash.len == 0:
        continue
      if hash == currentHash:
        continue
      let label = "history|" & hash
      seen.incl(label)
      libp2p_webtransport_cert_hash.set(
        1.0, labelValues = ["history", hash]
      )

    let maxSessionsOpt = s.webtransportMaxSessions()
    maxSessionsOpt.withValue(limit):
      libp2p_webtransport_max_sessions.set(float(limit))

    let interval = s.webtransportRotationInterval()
    if interval > chronos.ZeroDuration:
      let intervalSeconds =
        float(interval.nanoseconds()) / 1_000_000_000.0
      libp2p_webtransport_rotation_interval_seconds.set(intervalSeconds)
    libp2p_webtransport_rotation_keep_history.set(
      float(s.webtransportRotationKeepHistory())
    )
    let lastRotationOpt = s.webtransportLastRotation()
    if lastRotationOpt.isSome:
      libp2p_webtransport_last_rotation_timestamp.set(
        momentToUnixSeconds(lastRotationOpt.get())
      )
    else:
      libp2p_webtransport_last_rotation_timestamp.set(0.0)

    var readyDurationTotal = 0.0
    var pendingDurationTotal = 0.0
    var readyDurationCount = 0
    var pendingDurationCount = 0
    let nowMoment = Moment.now()
    let nowSeconds = momentToUnixSeconds(nowMoment)

    for snapshot in s.webtransportSessions():
      if snapshot.ready:
        inc readySessions
        snapshot.handshakeStart.withValue(startMoment):
          snapshot.handshakeReady.withValue(readyMoment):
            let duration =
              momentToUnixSeconds(readyMoment) - momentToUnixSeconds(startMoment)
            if duration >= 0.0:
              readyDurationTotal += duration
              inc readyDurationCount
      else:
        inc pendingSessions
        snapshot.handshakeStart.withValue(startMoment):
          let duration = nowSeconds - momentToUnixSeconds(startMoment)
          if duration >= 0.0:
            pendingDurationTotal += duration
            inc pendingDurationCount
    libp2p_webtransport_sessions.set(
      float(readySessions), labelValues = ["ready"]
    )
    libp2p_webtransport_sessions.set(
      float(pendingSessions), labelValues = ["pending"]
    )
    if readyDurationCount > 0:
      libp2p_webtransport_handshake_seconds.set(
        readyDurationTotal / float(readyDurationCount),
        labelValues = ["ready"],
      )
    if pendingDurationCount > 0:
      libp2p_webtransport_handshake_seconds.set(
        pendingDurationTotal / float(pendingDurationCount),
        labelValues = ["pending"],
      )
    let rejectionStats = s.webtransportRejectionStats()
    for reason in quictransport.WebtransportRejectionReason:
      let label = quictransport.webtransportRejectionReasonLabel(reason)
      let count = float(rejectionStats.count(reason))
      libp2p_webtransport_rejections_total.set(count, labelValues = [label])

    resetMissingWebtransportCerts(seen, cache)

proc initSwitchMetricsState*(): SwitchMetricsState {.inline.} =
  new result
  when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
    result.webtransport = initWebtransportMetricsState()
  result.resourceProtocols = initHashSet[string]()
  result.resourcePools = initHashSet[string]()
  result.bandwidthProtocols = initHashSet[string]()
  result.bandwidthGroups = initHashSet[string]()

proc updateIpnsMetrics*(
    trackers: openArray[KadDHT], state: IpnsMetricsState
) {.public.} =
  if state.isNil:
    return
  type IpnsNamespaceAggregate = object
    active: int
    backoff: int
    refreshSuccesses: uint64
    refreshFailures: uint64
    earliest: Option[chronos.Duration]
    lastSuccess: Option[DateTime]
    lastFailure: Option[DateTime]

  if trackers.len == 0:
    resetIpnsMetrics(state)
    return

  var totalTracked = 0
  var totalOverdue = 0
  var namespaceAgg = initTable[string, IpnsNamespaceAggregate]()
  var hasData = false

  for kad in trackers:
    if kad.isNil:
      continue
    let snapshotOpt = kad.ipnsRepublisherSnapshot()
    if snapshotOpt.isNone:
      continue
    hasData = true
    let snapshot = snapshotOpt.get()
    totalTracked += snapshot.totalRecords
    totalOverdue += snapshot.overdueRecords
    for nsSnap in snapshot.namespaces:
      let key = nsSnap.namespace
      withMapEntry(
        namespaceAgg,
        key,
        IpnsNamespaceAggregate(
          active: 0,
          backoff: 0,
          refreshSuccesses: 0,
          refreshFailures: 0,
          earliest: none(chronos.Duration),
          lastSuccess: none(DateTime),
          lastFailure: none(DateTime),
        ),
        agg
      ):
        agg.active += nsSnap.activeRecords
        agg.backoff += nsSnap.backoffRecords
        agg.refreshSuccesses += nsSnap.refreshSuccesses
        agg.refreshFailures += nsSnap.refreshFailures
        if nsSnap.earliestNextAttempt.isSome:
          let waitDur = nsSnap.earliestNextAttempt.get()
          if agg.earliest.isNone or waitDur < agg.earliest.get():
            agg.earliest = some(waitDur)
        if nsSnap.lastSuccess.isSome:
          let ts = nsSnap.lastSuccess.get()
          if agg.lastSuccess.isNone or agg.lastSuccess.get() < ts:
            agg.lastSuccess = some(ts)
        if nsSnap.lastFailure.isSome:
          let ts = nsSnap.lastFailure.get()
          if agg.lastFailure.isNone or agg.lastFailure.get() < ts:
            agg.lastFailure = some(ts)

  if not hasData:
    resetIpnsMetrics(state)
    return

  libp2p_ipns_records_total.set(float(totalTracked), labelValues = ["tracked"])
  libp2p_ipns_records_total.set(float(totalOverdue), labelValues = ["overdue"])

  var seenNamespaces = initHashSet[string]()
  for namespace, agg in namespaceAgg.pairs:
    seenNamespaces.incl(namespace)
    let label = ipnsLabel(namespace)
    libp2p_ipns_namespace_records.set(
      float(agg.active), labelValues = [label, "active"]
    )
    libp2p_ipns_namespace_records.set(
      float(agg.backoff), labelValues = [label, "backoff"]
    )
    libp2p_ipns_refresh_total.set(
      float(agg.refreshSuccesses), labelValues = [label, "success"]
    )
    libp2p_ipns_refresh_total.set(
      float(agg.refreshFailures), labelValues = [label, "failure"]
    )
    let nextSeconds =
      if agg.earliest.isSome:
        let waitNs = agg.earliest.get().nanoseconds()
        if waitNs <= 0:
          0.0
        else:
          float(waitNs) / 1_000_000_000.0
      else:
        0.0
    libp2p_ipns_next_attempt_seconds.set(nextSeconds, labelValues = [label])
    let lastSuccessUnix =
      if agg.lastSuccess.isSome:
        float(agg.lastSuccess.get().toTime().toUnix())
      else:
        0.0
    let lastFailureUnix =
      if agg.lastFailure.isSome:
        float(agg.lastFailure.get().toTime().toUnix())
      else:
        0.0
    libp2p_ipns_last_event_unix.set(
      lastSuccessUnix, labelValues = [label, "success"]
    )
    libp2p_ipns_last_event_unix.set(
      lastFailureUnix, labelValues = [label, "failure"]
    )

  resetMissingIpnsNamespaces(seenNamespaces, state)

proc updateIpnsMetrics*(tracker: KadDHT, state: IpnsMetricsState) {.public.} =
  if tracker.isNil:
    resetIpnsMetrics(state)
  else:
    var buf = @[tracker]
    updateIpnsMetrics(buf, state)

proc updateResourceMetrics*(
    snapshot: ResourceMetrics, state: SwitchMetricsState
) {.public, gcsafe.} =
  if state.isNil:
    return
  libp2p_resource_connections.set(snapshot.connInbound.int64, labelValues = ["inbound"])
  libp2p_resource_connections.set(snapshot.connOutbound.int64, labelValues = ["outbound"])
  libp2p_resource_streams.set(snapshot.streamInbound.int64, labelValues = ["inbound"])
  libp2p_resource_streams.set(snapshot.streamOutbound.int64, labelValues = ["outbound"])

  var seenProtocols = initHashSet[string]()
  for metric in snapshot.streamsByProtocol:
    if metric.protocol.len == 0:
      continue
    seenProtocols.incl(metric.protocol)
    libp2p_resource_streams_protocol.set(
      metric.inbound.int64, labelValues = [metric.protocol, "inbound"]
    )
    libp2p_resource_streams_protocol.set(
      metric.outbound.int64, labelValues = [metric.protocol, "outbound"]
    )
  resetMissingResourceProtocols(seenProtocols, state.resourceProtocols)

  var seenPools = initHashSet[string]()
  for poolMetric in snapshot.sharedPools:
    if poolMetric.pool.len == 0:
      continue
    seenPools.incl(poolMetric.pool)
    libp2p_resource_shared_pool_connections.set(
      poolMetric.connInbound.int64, labelValues = [poolMetric.pool, "inbound"]
    )
    libp2p_resource_shared_pool_connections.set(
      poolMetric.connOutbound.int64, labelValues = [poolMetric.pool, "outbound"]
    )
    libp2p_resource_shared_pool_streams.set(
      poolMetric.streamInbound.int64, labelValues = [poolMetric.pool, "inbound"]
    )
    libp2p_resource_shared_pool_streams.set(
      poolMetric.streamOutbound.int64, labelValues = [poolMetric.pool, "outbound"]
    )
  resetMissingResourcePools(seenPools, state.resourcePools)

proc updateBandwidthMetrics*(
    manager: BandwidthManager, state: SwitchMetricsState
) {.public, gcsafe.} =
  if manager.isNil or state.isNil:
    return

  let snapshot = manager.snapshot()
  libp2p_bandwidth_total_bytes.set(
    float(snapshot.totalInbound), labelValues = ["inbound"]
  )
  libp2p_bandwidth_total_bytes.set(
    float(snapshot.totalOutbound), labelValues = ["outbound"]
  )
  libp2p_bandwidth_rate_bps.set(snapshot.inboundRateBps, labelValues = ["inbound"])
  libp2p_bandwidth_rate_bps.set(snapshot.outboundRateBps, labelValues = ["outbound"])

  var seenProtocols = initHashSet[string]()
  for (proto, stats) in manager.protocols():
    if proto.len == 0:
      continue
    seenProtocols.incl(proto)
    libp2p_bandwidth_protocol_bytes.set(
      float(stats.inbound.totalBytes), labelValues = [proto, "inbound"]
    )
    libp2p_bandwidth_protocol_bytes.set(
      float(stats.outbound.totalBytes), labelValues = [proto, "outbound"]
    )
    libp2p_bandwidth_protocol_rate_bps.set(
      stats.inbound.emaBytesPerSecond, labelValues = [proto, "inbound"]
    )
    libp2p_bandwidth_protocol_rate_bps.set(
      stats.outbound.emaBytesPerSecond, labelValues = [proto, "outbound"]
    )
  resetMissingBandwidthProtocols(seenProtocols, state.bandwidthProtocols)

  let groupStats = manager.groupStats()
  var seenGroups = initHashSet[string]()
  for group, stats in groupStats.pairs():
    if group.len == 0:
      continue
    seenGroups.incl(group)
    libp2p_bandwidth_group_bytes.set(
      float(stats.inbound.totalBytes), labelValues = [group, "inbound"]
    )
    libp2p_bandwidth_group_bytes.set(
      float(stats.outbound.totalBytes), labelValues = [group, "outbound"]
    )
    libp2p_bandwidth_group_rate_bps.set(
      stats.inbound.emaBytesPerSecond, labelValues = [group, "inbound"]
    )
    libp2p_bandwidth_group_rate_bps.set(
      stats.outbound.emaBytesPerSecond, labelValues = [group, "outbound"]
    )
  resetMissingBandwidthGroups(seenGroups, state.bandwidthGroups)

proc updateGraphSyncMetrics*(s: Switch) {.public, gcsafe.}

proc updateSwitchMetrics*(
    s: Switch, state: SwitchMetricsState
) {.public, gcsafe.} =
  if state.isNil:
    return
  updateResourceMetrics(s.resourceSnapshot(), state)
  if not s.bandwidthManager.isNil:
    updateBandwidthMetrics(s.bandwidthManager, state)
  updateGraphSyncMetrics(s)
  when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
    updateWebtransportMetrics(s, state.webtransport)

proc updateBitswapAdvertiserMetrics(s: Switch) {.gcsafe.} =
  if s.bitswap.isNil or s.bitswap.store.isNil:
    resetBitswapAdvertiserMetrics()
    return
  if not (s.bitswap.store of AutoProviderBlockStore):
    resetBitswapAdvertiserMetrics()
    return

  let store = AutoProviderBlockStore(s.bitswap.store)
  let advStats = store.stats()

  libp2p_bitswap_advertisements_total.set(
    float(advStats.dhtSuccess), labelValues = ["dht", "success"]
  )
  libp2p_bitswap_advertisements_total.set(
    float(advStats.dhtFailure), labelValues = ["dht", "failure"]
  )
  libp2p_bitswap_advertisements_total.set(
    float(advStats.delegatedSuccess), labelValues = ["delegated", "success"]
  )
  libp2p_bitswap_advertisements_total.set(
    float(advStats.delegatedFailure), labelValues = ["delegated", "failure"]
  )
  libp2p_bitswap_advertisements_total.set(
    float(advStats.skipped), labelValues = [BitswapAdvertSkipTarget, "skipped"]
  )

  if advStats.lastAttempt.isSome:
    libp2p_bitswap_advertisement_last_attempt_timestamp.set(
      momentToUnixSeconds(advStats.lastAttempt.get())
    )
  else:
    libp2p_bitswap_advertisement_last_attempt_timestamp.set(0.0)

  if advStats.lastDhtSuccess.isSome:
    libp2p_bitswap_advertisement_last_success_timestamp.set(
      momentToUnixSeconds(advStats.lastDhtSuccess.get()), labelValues = ["dht"]
    )
  else:
    libp2p_bitswap_advertisement_last_success_timestamp.set(0.0, labelValues = ["dht"])

  if advStats.lastDelegatedSuccess.isSome:
    libp2p_bitswap_advertisement_last_success_timestamp.set(
      momentToUnixSeconds(advStats.lastDelegatedSuccess.get()),
      labelValues = ["delegated"],
    )
  else:
    libp2p_bitswap_advertisement_last_success_timestamp.set(
      0.0, labelValues = ["delegated"]
    )

proc updateBitswapMetrics*(
    s: Switch, state: BitswapMetricsState
) {.public, async.} =
  if state.isNil:
    return
  let statsOpt = await s.bitswapLedgerStats()
  if statsOpt.isNone:
    resetBitswapMetrics(state)
  else:
    let stats = statsOpt.get()
    libp2p_bitswap_bytes_total.set(
      float(stats.bytesReceived), labelValues = ["inbound"]
    )
    libp2p_bitswap_bytes_total.set(
      float(stats.bytesSent), labelValues = ["outbound"]
    )
    libp2p_bitswap_blocks_total.set(
      float(stats.blocksReceived), labelValues = ["inbound"]
    )
    libp2p_bitswap_blocks_total.set(
      float(stats.blocksSent), labelValues = ["outbound"]
    )
    libp2p_bitswap_have_total.set(
      float(stats.haveReceived), labelValues = ["inbound"]
    )
    libp2p_bitswap_have_total.set(
      float(stats.haveSent), labelValues = ["outbound"]
    )
    libp2p_bitswap_dont_have_total.set(
      float(stats.dontHaveReceived), labelValues = ["inbound"]
    )
    libp2p_bitswap_dont_have_total.set(
      float(stats.dontHaveSent), labelValues = ["outbound"]
    )
    libp2p_bitswap_wants_total.set(
      float(stats.wantsReceived), labelValues = ["inbound"]
    )
    libp2p_bitswap_wants_total.set(
      float(stats.wantsSent), labelValues = ["outbound"]
    )
    libp2p_bitswap_exchanges_total.set(float(stats.exchanges))
    libp2p_bitswap_peers.set(float(stats.peers))
    libp2p_bitswap_max_debt_ratio.set(stats.maxDebtRatio)

    var ratioSeen = initHashSet[string]()
    var bytesSeen = initHashSet[string]()
    let topDebtors = await s.bitswapTopDebtors(
      BitswapDefaultTopDebtorsLimit, BitswapDefaultTopDebtorsMinBytes
    )
    var rank = 1
    for (peerId, ratio, entry) in topDebtors:
      let peerStr = $peerId
      if peerStr.len == 0:
        continue
      let rankStr = $rank
      let ratioKey = peerStr & "|" & rankStr
      ratioSeen.incl(ratioKey)
      libp2p_bitswap_top_debtor_ratio.set(
        ratio,
        labelValues = [peerStr, rankStr],
      )
      let sentKey = ratioKey & "|sent"
      bytesSeen.incl(sentKey)
      libp2p_bitswap_top_debtor_bytes.set(
        float(entry.bytesSent),
        labelValues = [peerStr, rankStr, "sent"],
      )
      let receivedKey = ratioKey & "|received"
      bytesSeen.incl(receivedKey)
      libp2p_bitswap_top_debtor_bytes.set(
        float(entry.bytesReceived),
        labelValues = [peerStr, rankStr, "received"],
      )
      inc rank
    resetMissingBitswapTopDebtors(ratioSeen, bytesSeen, state)

  updateBitswapAdvertiserMetrics(s)

proc updateGraphSyncMetrics*(s: Switch) {.public.} =
  let statsOpt = s.graphSyncStats()
  if statsOpt.isNone:
    resetGraphSyncMetrics()
    return

  let stats = statsOpt.get()
  libp2p_graphsync_requests_total.set(
    float(stats.inboundRequests), labelValues = ["inbound", "total"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.inboundCompleted), labelValues = ["inbound", "completed"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.inboundPartial), labelValues = ["inbound", "partial"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.inboundFailed), labelValues = ["inbound", "failed"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.outboundRequests), labelValues = ["outbound", "total"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.outboundCompleted), labelValues = ["outbound", "completed"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.outboundPartial), labelValues = ["outbound", "partial"]
  )
  libp2p_graphsync_requests_total.set(
    float(stats.outboundFailed), labelValues = ["outbound", "failed"]
  )

  libp2p_graphsync_blocks_total.set(
    float(stats.inboundBlocks), labelValues = ["inbound"]
  )
  libp2p_graphsync_blocks_total.set(
    float(stats.outboundBlocks), labelValues = ["outbound"]
  )

  libp2p_graphsync_bytes_total.set(
    float(stats.inboundBytes), labelValues = ["inbound"]
  )
  libp2p_graphsync_bytes_total.set(
    float(stats.outboundBytes), labelValues = ["outbound"]
  )

  libp2p_graphsync_missing_links_total.set(
    float(stats.inboundMissing), labelValues = ["inbound"]
  )
  libp2p_graphsync_missing_links_total.set(
    float(stats.outboundMissing), labelValues = ["outbound"]
  )

proc spawnMetricsLoop*(
    s: Switch,
    interval: chronos.Duration = chronos.seconds(5),
    tracker: IpnsTrackerProc = nil
): Future[void] {.public.} =
  var bitswapState = initBitswapMetricsState()
  var ipnsState = initIpnsMetricsState()
  var switchState = initSwitchMetricsState()
  proc loop(): Future[void] {.async.} =
    try:
      while true:
        if s.isStarted():
          try:
            updateSwitchMetrics(s, switchState)
          except CatchableError as exc:
            warn "failed to update resource metrics", error = exc.msg
          try:
            await updateBitswapMetrics(s, bitswapState)
          except CatchableError as exc:
            warn "failed to update Bitswap metrics", error = exc.msg
          when not defined(libp2p_disable_datatransfer):
            try:
              await updateDataTransferMetrics(s)
            except CatchableError as exc:
              warn "failed to update DataTransfer metrics", error = exc.msg
          if tracker != nil:
            try:
              let tracked = tracker()
              updateIpnsMetrics(tracked, ipnsState)
            except CatchableError as exc:
              warn "failed to update IPNS metrics", error = exc.msg
        await sleepAsync(interval)
    except CancelledError:
      discard

  let fut = loop()
  asyncSpawn fut
  fut
