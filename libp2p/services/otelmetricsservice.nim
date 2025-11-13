# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[json, tables, times, strutils, options, sets]
import chronos, chronicles
import chronos/apps/http/httpclient

import ../switch
import ../bandwidthmanager
import ../resourcemanager
import ../protocols/bitswap/ledger
import ../protocols/kademlia/kademlia
import ../protocols/graphsync/graphsync
when not defined(libp2p_disable_datatransfer):
  import ../protocols/datatransfer/channelmanager
when defined(libp2p_msquic_experimental):
  import ../transports/msquictransport as quictransport
elif defined(libp2p_quic_support):
  import ../transports/quictransport
import ./otelutils
import ../utility

logScope:
  topics = "libp2p otelmetrics"

const
  DefaultOtelInterval = chronos.seconds(5)
  DefaultOtelTimeout = chronos.seconds(3)
  OtelAggregationTemporalityCumulative = 2

type
  OpenTelemetryExporterConfig* = object
    endpoint*: string
    headers*: seq[(string, string)]
    resourceAttributes*: seq[(string, string)]
    scopeName*: string
    scopeVersion*: string
    interval*: chronos.Duration
    timeout*: chronos.Duration

  OtelMetricsService* = ref object of Service
    config*: OpenTelemetryExporterConfig
    session: HttpSessionRef
    loopFuture: Future[void]
    startTimeUnixNano: uint64
    ipnsTrackers*: seq[KadDHT]
    bitswapTopDebtorRatios: HashSet[string]
    bitswapTopDebtorBytes: HashSet[string]

proc toJsonArray(nodes: seq[JsonNode]): JsonNode =
  result = newJArray()
  for node in nodes:
    result.add(node)


proc makeIntDataPoint(
    value: int64,
    timeUnixNano: uint64,
    attrs: seq[(string, string)],
    startTimeUnixNano: uint64 = 0,
): JsonNode =
  var node = newJObject()
  node["timeUnixNano"] = %int64(timeUnixNano)
  node["asInt"] = %value
  if startTimeUnixNano != 0:
    node["startTimeUnixNano"] = %int64(startTimeUnixNano)
  if attrs.len > 0:
    node["attributes"] = makeAttributes(attrs)
  node

proc makeDoubleDataPoint(
    value: float,
    timeUnixNano: uint64,
    attrs: seq[(string, string)],
): JsonNode =
  var node = newJObject()
  node["timeUnixNano"] = %int64(timeUnixNano)
  node["asDouble"] = %value
  if attrs.len > 0:
    node["attributes"] = makeAttributes(attrs)
  node

proc makeGaugeMetric(
    name, description: string, points: seq[JsonNode]
): JsonNode =
  if points.len == 0:
    return newJNull()
  var metric = newJObject()
  metric["name"] = %name
  if description.len > 0:
    metric["description"] = %description
  var gauge = newJObject()
  gauge["dataPoints"] = toJsonArray(points)
  metric["gauge"] = gauge
  metric

proc makeSumMetric(
    name, description: string, points: seq[JsonNode], monotonic: bool = true
): JsonNode =
  if points.len == 0:
    return newJNull()
  var metric = newJObject()
  metric["name"] = %name
  if description.len > 0:
    metric["description"] = %description
  var sum = newJObject()
  sum["dataPoints"] = toJsonArray(points)
  sum["aggregationTemporality"] = %OtelAggregationTemporalityCumulative
  sum["isMonotonic"] = %monotonic
  metric["sum"] = sum
  metric

proc ipnsLabel(namespace: string): string {.inline.} =
  if namespace.len == 0:
    "(root)"
  else:
    namespace

proc buildHeaders(config: OpenTelemetryExporterConfig): seq[(string, string)] =
  var headers = config.headers
  headers.add(("Accept", "application/json"))
  otelutils.buildHeaders(headers, "application/json")

proc collectResourceMetrics(snapshot: ResourceMetrics, timeNs: uint64): seq[JsonNode] =
  var metrics: seq[JsonNode] = @[]

  var connPoints = @[
    makeIntDataPoint(snapshot.connInbound.int64, timeNs, @[("direction", "inbound")]),
    makeIntDataPoint(snapshot.connOutbound.int64, timeNs, @[("direction", "outbound")]),
  ]
  metrics.add(
    makeGaugeMetric("libp2p.resource.connections", "Active libp2p connections", connPoints)
  )

  var streamPoints = @[
    makeIntDataPoint(snapshot.streamInbound.int64, timeNs, @[("direction", "inbound")]),
    makeIntDataPoint(snapshot.streamOutbound.int64, timeNs, @[("direction", "outbound")]),
  ]
  metrics.add(
    makeGaugeMetric("libp2p.resource.streams", "Active libp2p streams", streamPoints)
  )

  var protocolPoints: seq[JsonNode] = @[]
  for protoMetric in snapshot.streamsByProtocol:
    if protoMetric.protocol.len == 0:
      continue
    protocolPoints.add(
      makeIntDataPoint(
        protoMetric.inbound.int64,
        timeNs,
        @[("protocol", protoMetric.protocol), ("direction", "inbound")],
      )
    )
    protocolPoints.add(
      makeIntDataPoint(
        protoMetric.outbound.int64,
        timeNs,
        @[("protocol", protoMetric.protocol), ("direction", "outbound")],
      )
    )
  metrics.add(
    makeGaugeMetric(
      "libp2p.resource.streams_by_protocol",
      "Active libp2p streams per protocol",
      protocolPoints,
    )
  )

  var poolConnPoints: seq[JsonNode] = @[]
  var poolStreamPoints: seq[JsonNode] = @[]
  for poolMetric in snapshot.sharedPools:
    if poolMetric.pool.len == 0:
      continue
    poolConnPoints.add(
      makeIntDataPoint(
        poolMetric.connInbound.int64,
        timeNs,
        @[("pool", poolMetric.pool), ("direction", "inbound")],
      )
    )
    poolConnPoints.add(
      makeIntDataPoint(
        poolMetric.connOutbound.int64,
        timeNs,
        @[("pool", poolMetric.pool), ("direction", "outbound")],
      )
    )
    poolStreamPoints.add(
      makeIntDataPoint(
        poolMetric.streamInbound.int64,
        timeNs,
        @[("pool", poolMetric.pool), ("direction", "inbound")],
      )
    )
    poolStreamPoints.add(
      makeIntDataPoint(
        poolMetric.streamOutbound.int64,
        timeNs,
        @[("pool", poolMetric.pool), ("direction", "outbound")],
      )
    )
  if poolConnPoints.len > 0:
    metrics.add(
      makeGaugeMetric(
        "libp2p.resource.shared_pool.connections",
        "Resource manager shared pool connections",
        poolConnPoints,
      )
    )
  if poolStreamPoints.len > 0:
    metrics.add(
      makeGaugeMetric(
        "libp2p.resource.shared_pool.streams",
        "Resource manager shared pool streams",
        poolStreamPoints,
      )
    )

  metrics

proc collectBandwidthMetrics(
    manager: BandwidthManager, timeNs, startNs: uint64
): seq[JsonNode] =
  var metrics: seq[JsonNode] = @[]
  if manager.isNil:
    return metrics

  let snapshot = manager.snapshot()

  let totalPoints = @[
    makeIntDataPoint(
      clampToInt64(snapshot.totalInbound),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(snapshot.totalOutbound),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bandwidth.total_bytes",
      "Cumulative libp2p bandwidth usage (bytes)",
      totalPoints,
    )
  )

  let ratePoints = @[
    makeDoubleDataPoint(snapshot.inboundRateBps, timeNs, @[("direction", "inbound")]),
    makeDoubleDataPoint(snapshot.outboundRateBps, timeNs, @[("direction", "outbound")]),
  ]
  metrics.add(
    makeGaugeMetric(
      "libp2p.bandwidth.rate_bps",
      "Estimated libp2p bandwidth rate (bytes/s)",
      ratePoints,
    )
  )

  var protoBytesPoints: seq[JsonNode] = @[]
  var protoRatePoints: seq[JsonNode] = @[]
  for protoEntry in manager.protocols():
    let (protoName, stats) = protoEntry
    if protoName.len == 0:
      continue
    protoBytesPoints.add(
      makeIntDataPoint(
        clampToInt64(stats.inbound.totalBytes),
        timeNs,
        @[("protocol", protoName), ("direction", "inbound")],
        startNs,
      )
    )
    protoBytesPoints.add(
      makeIntDataPoint(
        clampToInt64(stats.outbound.totalBytes),
        timeNs,
        @[("protocol", protoName), ("direction", "outbound")],
        startNs,
      )
    )
    protoRatePoints.add(
      makeDoubleDataPoint(
        stats.inbound.emaBytesPerSecond,
        timeNs,
        @[("protocol", protoName), ("direction", "inbound")],
      )
    )
    protoRatePoints.add(
      makeDoubleDataPoint(
        stats.outbound.emaBytesPerSecond,
        timeNs,
        @[("protocol", protoName), ("direction", "outbound")],
      )
    )
  if protoBytesPoints.len > 0:
    metrics.add(
      makeSumMetric(
        "libp2p.bandwidth.protocol_bytes",
        "Cumulative libp2p bandwidth per protocol (bytes)",
        protoBytesPoints,
      )
    )
  if protoRatePoints.len > 0:
    metrics.add(
      makeGaugeMetric(
        "libp2p.bandwidth.protocol_rate_bps",
        "Estimated libp2p bandwidth rate per protocol (bytes/s)",
        protoRatePoints,
      )
    )

  var groupBytesPoints: seq[JsonNode] = @[]
  var groupRatePoints: seq[JsonNode] = @[]
  for group, stats in manager.groupStats().pairs():
    if group.len == 0:
      continue
    groupBytesPoints.add(
      makeIntDataPoint(
        clampToInt64(stats.inbound.totalBytes),
        timeNs,
        @[("group", group), ("direction", "inbound")],
        startNs,
      )
    )
    groupBytesPoints.add(
      makeIntDataPoint(
        clampToInt64(stats.outbound.totalBytes),
        timeNs,
        @[("group", group), ("direction", "outbound")],
        startNs,
      )
    )
    groupRatePoints.add(
      makeDoubleDataPoint(
        stats.inbound.emaBytesPerSecond,
        timeNs,
        @[("group", group), ("direction", "inbound")],
      )
    )
    groupRatePoints.add(
      makeDoubleDataPoint(
        stats.outbound.emaBytesPerSecond,
        timeNs,
        @[("group", group), ("direction", "outbound")],
      )
    )
  if groupBytesPoints.len > 0:
    metrics.add(
      makeSumMetric(
        "libp2p.bandwidth.group_bytes",
        "Cumulative libp2p bandwidth per group (bytes)",
        groupBytesPoints,
      )
    )
  if groupRatePoints.len > 0:
    metrics.add(
      makeGaugeMetric(
        "libp2p.bandwidth.group_rate_bps",
        "Estimated libp2p bandwidth rate per group (bytes/s)",
        groupRatePoints,
      )
    )

  metrics

proc collectBitswapMetrics(
    self: OtelMetricsService,
    statsOpt: Option[BitswapLedgerStats],
    topDebtors: seq[(PeerId, float, BitswapPeerLedger)],
    timeNs, startNs: uint64
): seq[JsonNode] =
  var metrics: seq[JsonNode] = @[]
  let stats = statsOpt.valueOr:
    return metrics

  var bytesPoints = @[
    makeIntDataPoint(
      clampToInt64(stats.bytesReceived),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.bytesSent),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bitswap.bytes_total",
      "Cumulative Bitswap bytes recorded by ledger",
      bytesPoints,
    )
  )

  var blockPoints = @[
    makeIntDataPoint(
      clampToInt64(stats.blocksReceived),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.blocksSent),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bitswap.blocks_total",
      "Cumulative Bitswap blocks recorded by ledger",
      blockPoints,
    )
  )

  var havePoints = @[
    makeIntDataPoint(
      clampToInt64(stats.haveReceived),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.haveSent),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bitswap.have_total",
      "Bitswap HAVE signals recorded by ledger",
      havePoints,
    )
  )

  var dontHavePoints = @[
    makeIntDataPoint(
      clampToInt64(stats.dontHaveReceived),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.dontHaveSent),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bitswap.dont_have_total",
      "Bitswap DONT_HAVE signals recorded by ledger",
      dontHavePoints,
    )
  )

  var wantPoints = @[
    makeIntDataPoint(
      clampToInt64(stats.wantsReceived),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.wantsSent),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bitswap.wants_total",
      "Bitswap wants processed by ledger",
      wantPoints,
    )
  )

  var exchangePoints = @[
    makeIntDataPoint(
      clampToInt64(stats.exchanges),
      timeNs,
      @[],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.bitswap.exchanges_total",
      "Bitswap exchanges recorded by ledger",
      exchangePoints,
    )
  )

  var peerPoints = @[
    makeIntDataPoint(
      stats.peers.int64,
      timeNs,
      @[],
    ),
  ]
  metrics.add(
    makeGaugeMetric(
      "libp2p.bitswap.peers",
      "Bitswap peers tracked by ledger",
      peerPoints,
    )
  )

  var maxDebtPoints = @[
    makeDoubleDataPoint(stats.maxDebtRatio, timeNs, @[]),
  ]
  metrics.add(
    makeGaugeMetric(
      "libp2p.bitswap.max_debt_ratio",
      "Maximum Bitswap debt ratio across peers",
      maxDebtPoints,
    )
  )

  var ratioPoints: seq[JsonNode] = @[]
  var debtorBytesPoints: seq[JsonNode] = @[]
  var ratioSeen = initHashSet[string]()
  var bytesSeen = initHashSet[string]()
  var rank = 1
  for (peerId, ratio, entry) in topDebtors:
    let peerStr = $peerId
    if peerStr.len == 0:
      continue
    let rankStr = $rank
    let ratioKey = peerStr & "|" & rankStr
    ratioSeen.incl(ratioKey)
    ratioPoints.add(
      makeDoubleDataPoint(
        ratio,
        timeNs,
        @[("peer", peerStr), ("rank", rankStr)],
      )
    )
    let sentKey = ratioKey & "|sent"
    bytesSeen.incl(sentKey)
    debtorBytesPoints.add(
      makeIntDataPoint(
        clampToInt64(entry.bytesSent),
        timeNs,
        @[("peer", peerStr), ("rank", rankStr), ("direction", "sent")],
        startNs,
      )
    )
    let receivedKey = ratioKey & "|received"
    bytesSeen.incl(receivedKey)
    debtorBytesPoints.add(
      makeIntDataPoint(
        clampToInt64(entry.bytesReceived),
        timeNs,
        @[("peer", peerStr), ("rank", rankStr), ("direction", "received")],
        startNs,
      )
    )
    inc rank

  for label in self.bitswapTopDebtorRatios:
    if label notin ratioSeen:
      var sep = -1
      for idx in 0 ..< label.len:
        if label[idx] == '|':
          sep = idx
          break
      if sep <= 0 or sep >= label.len - 1:
        continue
      let peer = label[0 ..< sep]
      let rankStr = label[sep + 1 .. ^1]
      ratioPoints.add(
        makeDoubleDataPoint(
          0.0,
          timeNs,
          @[("peer", peer), ("rank", rankStr)],
        )
      )

  for label in self.bitswapTopDebtorBytes:
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
      let rankStr = label[firstSep + 1 ..< secondSep]
      let direction = label[secondSep + 1 .. ^1]
      debtorBytesPoints.add(
        makeIntDataPoint(
          0,
          timeNs,
          @[("peer", peer), ("rank", rankStr), ("direction", direction)],
          startNs,
        )
      )

  if ratioPoints.len > 0:
    metrics.add(
      makeGaugeMetric(
        "libp2p.bitswap.top_debtor_ratio",
        "Bitswap debt ratio for the top debtor peers",
        ratioPoints,
      )
    )
  if debtorBytesPoints.len > 0:
    metrics.add(
      makeSumMetric(
        "libp2p.bitswap.top_debtor_bytes",
        "Bitswap bytes exchanged with the top debtor peers",
        debtorBytesPoints,
      )
    )

  self.bitswapTopDebtorRatios = ratioSeen
  self.bitswapTopDebtorBytes = bytesSeen

  metrics

proc collectGraphSyncMetrics(
    statsOpt: Option[GraphSyncStats], timeNs, startNs: uint64
): seq[JsonNode] =
  var metrics: seq[JsonNode] = @[]
  let stats = statsOpt.valueOr:
    return metrics

  var requestPoints = @[
    makeIntDataPoint(
      clampToInt64(stats.inboundRequests),
      timeNs,
      @[("direction", "inbound"), ("outcome", "total")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.inboundCompleted),
      timeNs,
      @[("direction", "inbound"), ("outcome", "completed")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.inboundPartial),
      timeNs,
      @[("direction", "inbound"), ("outcome", "partial")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.inboundFailed),
      timeNs,
      @[("direction", "inbound"), ("outcome", "failed")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundRequests),
      timeNs,
      @[("direction", "outbound"), ("outcome", "total")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundCompleted),
      timeNs,
      @[("direction", "outbound"), ("outcome", "completed")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundPartial),
      timeNs,
      @[("direction", "outbound"), ("outcome", "partial")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundFailed),
      timeNs,
      @[("direction", "outbound"), ("outcome", "failed")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.graphsync.requests_total",
      "GraphSync requests handled",
      requestPoints,
    )
  )

  var blockPoints = @[
    makeIntDataPoint(
      clampToInt64(stats.inboundBlocks),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundBlocks),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.graphsync.blocks_total",
      "GraphSync blocks transferred",
      blockPoints,
    )
  )

  var bytePoints = @[
    makeIntDataPoint(
      clampToInt64(stats.inboundBytes),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundBytes),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.graphsync.bytes_total",
      "GraphSync bytes transferred",
      bytePoints,
    )
  )

  var missingPoints = @[
    makeIntDataPoint(
      clampToInt64(stats.inboundMissing),
      timeNs,
      @[("direction", "inbound")],
      startNs,
    ),
    makeIntDataPoint(
      clampToInt64(stats.outboundMissing),
      timeNs,
      @[("direction", "outbound")],
      startNs,
    ),
  ]
  metrics.add(
    makeSumMetric(
      "libp2p.graphsync.missing_links_total",
      "GraphSync links reported as missing",
      missingPoints,
    )
  )

  metrics

when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
  proc collectWebtransportMetrics(
      switch: Switch, timeNs, startNs: uint64
  ): seq[JsonNode] =
    var metrics: seq[JsonNode] = @[]
    if switch.isNil:
      return metrics
    discard startNs

    var certPoints: seq[JsonNode] = @[]
    var currentHash = ""
    let currentOpt = switch.webtransportCerthash()
    if currentOpt.isSome:
      currentHash = currentOpt.get()
      if currentHash.len > 0:
        certPoints.add(
          makeDoubleDataPoint(
            1.0,
            timeNs,
            @[("state", "current"), ("hash", currentHash)],
          )
        )
    for hash in switch.webtransportCerthashHistory():
      if hash.len == 0 or hash == currentHash:
        continue
      certPoints.add(
        makeDoubleDataPoint(
          1.0,
          timeNs,
          @[("state", "history"), ("hash", hash)],
        )
      )
    if certPoints.len > 0:
      metrics.add(
        makeGaugeMetric(
          "libp2p.webtransport.cert_hash",
          "WebTransport certificate hashes exposed to clients",
          certPoints,
        )
      )

    var readySessions: uint64 = 0
    var pendingSessions: uint64 = 0
    var readyDurationTotal = 0.0
    var readyDurationCount = 0
    var pendingDurationTotal = 0.0
    var pendingDurationCount = 0
    let nowSeconds =
      float(Moment.now().epochNanoSeconds()) / 1_000_000_000.0
    for snapshot in switch.webtransportSessions():
      if snapshot.ready:
        inc readySessions
        snapshot.handshakeStart.withValue(startMoment):
          snapshot.handshakeReady.withValue(readyMoment):
            let duration =
              float(readyMoment.epochNanoSeconds() - startMoment.epochNanoSeconds()) /
              1_000_000_000.0
            if duration >= 0.0:
              readyDurationTotal += duration
              inc readyDurationCount
      else:
        inc pendingSessions
        snapshot.handshakeStart.withValue(startMoment):
          let duration =
            nowSeconds -
            float(startMoment.epochNanoSeconds()) / 1_000_000_000.0
          if duration >= 0.0:
            pendingDurationTotal += duration
            inc pendingDurationCount
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.sessions",
        "WebTransport session count grouped by readiness state",
        @[
          makeIntDataPoint(
            clampToInt64(readySessions),
            timeNs,
            @[("state", "ready")],
          ),
          makeIntDataPoint(
            clampToInt64(pendingSessions),
            timeNs,
            @[("state", "pending")],
          ),
        ],
      )
    )
    let readyAverage =
      if readyDurationCount > 0:
        readyDurationTotal / float(readyDurationCount)
      else:
        0.0
    let pendingAverage =
      if pendingDurationCount > 0:
        pendingDurationTotal / float(pendingDurationCount)
      else:
        0.0
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.handshake_seconds",
        "Average WebTransport handshake durations grouped by state",
        @[
          makeDoubleDataPoint(
            readyAverage,
            timeNs,
            @[("state", "ready")],
          ),
          makeDoubleDataPoint(
            pendingAverage,
            timeNs,
            @[("state", "pending")],
          ),
        ],
      )
    )

    let rejectionStats = switch.webtransportRejectionStats()
    var rejectionPoints: seq[JsonNode] = @[]
    for reason in quictransport.WebtransportRejectionReason:
      let label = quictransport.webtransportRejectionReasonLabel(reason)
      let value = clampToInt64(rejectionStats.count(reason))
      rejectionPoints.add(
        makeIntDataPoint(
          value,
          timeNs,
          @[("reason", label)],
        )
      )
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.rejections_total",
        "WebTransport handshake rejections grouped by reason",
        rejectionPoints,
      )
    )

    let limitOpt = switch.webtransportMaxSessions()
    let limitVal =
      if limitOpt.isSome: limitOpt.get().uint64 else: 0'u64
    var sessionPoints = @[
      makeIntDataPoint(
        clampToInt64(limitVal),
        timeNs,
        @[],
      ),
    ]
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.max_sessions",
        "Configured WebTransport session concurrency limit",
        sessionPoints,
      )
    )

    let rotationInterval = switch.webtransportRotationInterval()
    let intervalSeconds =
      if rotationInterval > chronos.ZeroDuration:
        float(rotationInterval.nanoseconds()) / 1_000_000_000.0
      else:
        0.0
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.rotation_interval_seconds",
        "Configured WebTransport certificate rotation interval (seconds)",
        @[
          makeDoubleDataPoint(
            intervalSeconds,
            timeNs,
            @[],
          ),
        ],
      )
    )

    let keepHistory = switch.webtransportRotationKeepHistory()
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.rotation_keep_history",
        "Configured WebTransport certificate rotation history size",
        @[
          makeIntDataPoint(
            clampToInt64(uint64(keepHistory)),
            timeNs,
            @[],
          ),
        ],
      )
    )

    let lastRotationOpt = switch.webtransportLastRotation()
    let lastRotationSeconds =
      if lastRotationOpt.isSome:
        float(lastRotationOpt.get().epochNanoSeconds()) / 1_000_000_000.0
      else:
        0.0
    metrics.add(
      makeGaugeMetric(
        "libp2p.webtransport.last_rotation_unix",
        "Unix timestamp of the last successful WebTransport certificate rotation",
        @[
          makeDoubleDataPoint(
            lastRotationSeconds,
            timeNs,
            @[],
          ),
        ],
      )
    )

    metrics

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

  proc collectDataTransferMetrics(
      statsOpt: Option[DataTransferStats], timeNs, startNs: uint64
  ): seq[JsonNode] =
    var metrics: seq[JsonNode] = @[]
    if statsOpt.isNone:
      return metrics

    let stats = statsOpt.get()

    var channelPoints: seq[JsonNode] = @[]
    for dir in DataTransferChannelDirection:
      let dirLabel = dataTransferDirectionLabel(dir)
      for status in DataTransferChannelStatus:
        let value = stats.counts[dir][status]
        channelPoints.add(
          makeIntDataPoint(
            int64(value),
            timeNs,
            @[("direction", dirLabel), ("status", dataTransferStatusLabel(status))],
            startNs,
          )
        )
    metrics.add(
      makeGaugeMetric(
        "libp2p.datatransfer.channels",
        "Data transfer channels by status",
        channelPoints,
      )
    )

    var pausedPoints: seq[JsonNode] = @[]
    for dir in DataTransferChannelDirection:
      let dirLabel = dataTransferDirectionLabel(dir)
      pausedPoints.add(
        makeIntDataPoint(
          int64(stats.pausedLocal[dir]),
          timeNs,
          @[("direction", dirLabel), ("kind", "local")],
          startNs,
        )
      )
      pausedPoints.add(
        makeIntDataPoint(
          int64(stats.pausedRemote[dir]),
          timeNs,
          @[("direction", dirLabel), ("kind", "remote")],
          startNs,
        )
      )
    metrics.add(
      makeGaugeMetric(
        "libp2p.datatransfer.paused",
        "Data transfer channels paused state",
        pausedPoints,
      )
    )

    metrics.add(
      makeGaugeMetric(
        "libp2p.datatransfer.channels_total",
        "Total data transfer channels tracked",
        @[
          makeIntDataPoint(int64(stats.total), timeNs, @[], startNs)
        ],
      )
    )

    metrics

proc collectIpnsMetrics(trackers: openArray[KadDHT], timeNs: uint64): seq[JsonNode] =
  type NamespaceAggregate = object
    active: int
    backoff: int
    refreshSuccesses: uint64
    refreshFailures: uint64
    earliest: Option[chronos.Duration]
    lastSuccess: Option[DateTime]
    lastFailure: Option[DateTime]

  var metrics: seq[JsonNode] = @[]
  if trackers.len == 0:
    return metrics

  var totalTracked = 0
  var totalOverdue = 0
  var namespaceAgg = initTable[string, NamespaceAggregate]()
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
    for ns in snapshot.namespaces:
      withMapEntry(
        namespaceAgg,
        ns.namespace,
        NamespaceAggregate(
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
        agg.active += ns.activeRecords
        agg.backoff += ns.backoffRecords
        agg.refreshSuccesses += ns.refreshSuccesses
        agg.refreshFailures += ns.refreshFailures
        if ns.earliestNextAttempt.isSome:
          let waitDur = ns.earliestNextAttempt.get()
          if agg.earliest.isNone or waitDur < agg.earliest.get():
            agg.earliest = some(waitDur)
        if ns.lastSuccess.isSome:
          let ts = ns.lastSuccess.get()
          if agg.lastSuccess.isNone or agg.lastSuccess.get() < ts:
            agg.lastSuccess = some(ts)
        if ns.lastFailure.isSome:
          let ts = ns.lastFailure.get()
          if agg.lastFailure.isNone or agg.lastFailure.get() < ts:
            agg.lastFailure = some(ts)

  if not hasData:
    return metrics

  var totalPoints = @[
    makeIntDataPoint(
      clampToInt64(uint64(totalTracked)),
      timeNs,
      @[("state", "tracked")],
    ),
    makeIntDataPoint(
      clampToInt64(uint64(totalOverdue)),
      timeNs,
      @[("state", "overdue")],
    ),
  ]
  metrics.add(
    makeGaugeMetric(
      "libp2p.ipns.records_total",
      "IPNS records tracked by republisher",
      totalPoints,
    )
  )

  var namespacePoints: seq[JsonNode] = @[]
  var refreshPoints: seq[JsonNode] = @[]
  var nextAttemptPoints: seq[JsonNode] = @[]
  var lastEventPoints: seq[JsonNode] = @[]

  for namespace, agg in namespaceAgg.pairs():
    let label = ipnsLabel(namespace)
    namespacePoints.add(
      makeIntDataPoint(
        clampToInt64(uint64(agg.active)),
        timeNs,
        @[("namespace", label), ("state", "active")],
      )
    )
    namespacePoints.add(
      makeIntDataPoint(
        clampToInt64(uint64(agg.backoff)),
        timeNs,
        @[("namespace", label), ("state", "backoff")],
      )
    )
    refreshPoints.add(
      makeIntDataPoint(
        clampToInt64(agg.refreshSuccesses),
        timeNs,
        @[("namespace", label), ("result", "success")],
      )
    )
    refreshPoints.add(
      makeIntDataPoint(
        clampToInt64(agg.refreshFailures),
        timeNs,
        @[("namespace", label), ("result", "failure")],
      )
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
    nextAttemptPoints.add(
      makeDoubleDataPoint(
        nextSeconds,
        timeNs,
        @[("namespace", label)],
      )
    )

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
    lastEventPoints.add(
      makeDoubleDataPoint(
        lastSuccessUnix,
        timeNs,
        @[("namespace", label), ("event", "success")],
      )
    )
    lastEventPoints.add(
      makeDoubleDataPoint(
        lastFailureUnix,
        timeNs,
        @[("namespace", label), ("event", "failure")],
      )
    )

  metrics.add(
    makeGaugeMetric(
      "libp2p.ipns.namespace_records",
      "IPNS records per namespace",
      namespacePoints,
    )
  )
  metrics.add(
    makeGaugeMetric(
      "libp2p.ipns.refresh_total",
      "IPNS refresh outcomes per namespace",
      refreshPoints,
    )
  )
  metrics.add(
    makeGaugeMetric(
      "libp2p.ipns.next_attempt_seconds",
      "Seconds until the earliest scheduled IPNS refresh per namespace",
      nextAttemptPoints,
    )
  )
  metrics.add(
    makeGaugeMetric(
      "libp2p.ipns.last_event_unix",
      "Unix timestamp of the last IPNS refresh event",
      lastEventPoints,
    )
  )

  metrics

proc buildMetricsPayload(
    self: OtelMetricsService,
    switch: Switch,
    timeNs: uint64,
    bitswapStats: Option[BitswapLedgerStats],
    topDebtors: seq[(PeerId, float, BitswapPeerLedger)],
    extraMetrics: seq[JsonNode] = @[]
): string =
  let resourceMetrics = switch.resourceSnapshot()
  let resourceMetricNodes = collectResourceMetrics(resourceMetrics, timeNs)

  var metricNodes = resourceMetricNodes
  if not switch.bandwidthManager.isNil:
    metricNodes.add(
      collectBandwidthMetrics(switch.bandwidthManager, timeNs, self.startTimeUnixNano)
    )
  let graphSyncStats = switch.graphSyncStats()
  metricNodes.add(collectGraphSyncMetrics(graphSyncStats, timeNs, self.startTimeUnixNano))
  when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
    metricNodes.add(
      collectWebtransportMetrics(switch, timeNs, self.startTimeUnixNano)
    )
  metricNodes.add(
    self.collectBitswapMetrics(bitswapStats, topDebtors, timeNs, self.startTimeUnixNano)
  )
  for node in extraMetrics:
    metricNodes.add(node)
  if self.ipnsTrackers.len > 0:
    metricNodes.add(collectIpnsMetrics(self.ipnsTrackers, timeNs))

  if metricNodes.len == 0:
    return ""

  var flatMetrics: seq[JsonNode] = @[]
  for node in metricNodes:
    if node.kind != JNull:
      flatMetrics.add(node)

  if flatMetrics.len == 0:
    return ""

  var scope = newJObject()
  scope["name"] = %self.config.scopeName
  if self.config.scopeVersion.len > 0:
    scope["version"] = %self.config.scopeVersion

  var scopeMetrics = newJObject()
  scopeMetrics["scope"] = scope
  scopeMetrics["metrics"] = toJsonArray(flatMetrics)

  var scopeMetricsArray = newJArray()
  scopeMetricsArray.add(scopeMetrics)

  var resource = newJObject()
  resource["attributes"] = makeAttributes(
    normalizedResourceAttributes(self.config.resourceAttributes, self.config.scopeVersion)
  )

  var resourceMetricsEntry = newJObject()
  resourceMetricsEntry["resource"] = resource
  resourceMetricsEntry["scopeMetrics"] = scopeMetricsArray

  var resourceMetricsArray = newJArray()
  resourceMetricsArray.add(resourceMetricsEntry)

  var payload = newJObject()
  payload["resourceMetrics"] = resourceMetricsArray

  $payload

proc sendPayload(
    self: OtelMetricsService, payload: string
): Future[bool] {.async: (raises: [CancelledError, CatchableError]).} =
  if payload.len == 0:
    return true

  if self.session.isNil:
    self.session = HttpSessionRef.new()

  try:
    let headers = buildHeaders(self.config)
    let requestResult = HttpClientRequestRef.post(
      self.session,
      self.config.endpoint,
      body = payload,
      headers = headers,
    )

    if requestResult.isErr:
      warn "OpenTelemetry metrics exporter failed to create request",
        error = $requestResult.error
      return false

    let request = requestResult.unsafeGet()
    let responseFuture = request.send()

    if not await responseFuture.withTimeout(self.config.timeout):
      warn "OpenTelemetry metrics exporter request timed out",
        timeout = $self.config.timeout
      return false

    let response = await responseFuture
    defer:
      await response.closeWait()

    if response.status < 200 or response.status >= 300:
      let bodyBytes = await response.getBodyBytes()
      var snippet = ""
      let limit = min(bodyBytes.len, 512)
      for i in 0 ..< limit:
        snippet.add(char(bodyBytes[i]))
      warn "OpenTelemetry metrics exporter received non-success response",
        status = response.status, body = snippet
      return false

    true
  except CancelledError as exc:
    raise exc
  except AsyncTimeoutError:
    warn "OpenTelemetry metrics exporter request timed out", timeout = $self.config.timeout
    return false
  except HttpError as exc:
    warn "OpenTelemetry metrics exporter request failed", error = exc.msg
    return false
  except CatchableError as exc:
    warn "OpenTelemetry metrics exporter encountered unexpected error", error = exc.msg
    return false

proc pushMetricsInner(
    self: OtelMetricsService, switch: Switch
): Future[void] {.async: (raises: [CancelledError]).} =
  if self.startTimeUnixNano == 0:
    self.startTimeUnixNano = unixNowNs()
  let timeNs = unixNowNs()
  let bitswapStats = await switch.bitswapLedgerStats()
  var topDebtors: seq[(PeerId, float, BitswapPeerLedger)] = @[]
  if bitswapStats.isSome:
    topDebtors = await switch.bitswapTopDebtors(
      BitswapDefaultTopDebtorsLimit, BitswapDefaultTopDebtorsMinBytes
    )
  var extraMetrics: seq[JsonNode] = @[]
  when not defined(libp2p_disable_datatransfer):
    let dataTransferStats = await switch.dataTransferStats()
    let dtMetrics = collectDataTransferMetrics(
      dataTransferStats, timeNs, self.startTimeUnixNano
    )
    for metric in dtMetrics:
      extraMetrics.add(metric)
  let payload = self.buildMetricsPayload(
    switch, timeNs, bitswapStats, topDebtors, extraMetrics
  )
  if payload.len == 0:
    return
  try:
    let sendOk = await self.sendPayload(payload)
    if not sendOk:
      return
  except CatchableError as exc:
    warn "OpenTelemetry metrics exporter send failed", error = exc.msg
    return

proc loop(
    self: OtelMetricsService, switch: Switch
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    while true:
      if switch.isStarted():
        try:
          await self.pushMetricsInner(switch)
        except CancelledError as exc:
          raise exc
        except CatchableError as exc:
          warn "OpenTelemetry metrics exporter loop iteration failed", error = exc.msg
      await sleepAsync(self.config.interval)
  except CancelledError as exc:
    discard
  except CatchableError as exc:
    warn "OpenTelemetry metrics exporter loop error", error = exc.msg

proc new*(
    T: typedesc[OtelMetricsService],
    endpoint: string,
    headers: seq[(string, string)] = @[],
    resourceAttributes: seq[(string, string)] = @[],
    scopeName: string = "nim-libp2p",
    scopeVersion: string = "",
    interval: chronos.Duration = DefaultOtelInterval,
    timeout: chronos.Duration = DefaultOtelTimeout,
): T {.raises: [ValueError].} =
  let trimmedEndpoint = endpoint.strip()
  if trimmedEndpoint.len == 0:
    raise newException(ValueError, "OpenTelemetry exporter endpoint must not be empty")

  {.push raises: [ValueError].}
  var cfg: OpenTelemetryExporterConfig
  cfg.endpoint = trimmedEndpoint
  cfg.headers = headers
  cfg.resourceAttributes = resourceAttributes
  cfg.scopeName = scopeName.strip()
  cfg.scopeVersion = scopeVersion.strip()
  cfg.interval = interval
  cfg.timeout = timeout

  if cfg.scopeName.len == 0:
    cfg.scopeName = "nim-libp2p"

  if cfg.interval <= chronos.ZeroDuration:
    cfg.interval = DefaultOtelInterval

  if cfg.timeout <= chronos.ZeroDuration:
    cfg.timeout = DefaultOtelTimeout
  {.pop.}

  T(
    config: cfg,
    bitswapTopDebtorRatios: initHashSet[string](),
    bitswapTopDebtorBytes: initHashSet[string](),
  )

proc registerIpnsTracker*(
    self: OtelMetricsService, kad: KadDHT
) {.public.} =
  if kad.isNil:
    return
  if kad notin self.ipnsTrackers:
    self.ipnsTrackers.add(kad)

method setup*(
    self: OtelMetricsService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(self).setup(switch)
  if hasBeenSetup and self.session.isNil:
    self.session = HttpSessionRef.new()
  hasBeenSetup

method run*(
    self: OtelMetricsService, switch: Switch
) {.base, async: (raises: [CancelledError, Exception]).} =
  if self.session.isNil:
    self.session = HttpSessionRef.new()

  if self.loopFuture.isNil:
    self.startTimeUnixNano = unixNowNs()
    try:
      if switch.isStarted():
        await self.pushMetricsInner(switch)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "OpenTelemetry metrics exporter initial push failed", error = exc.msg
    self.loopFuture = self.loop(switch)
    asyncSpawn(self.loopFuture)

method stop*(
    self: OtelMetricsService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(self).stop(switch)
  if hasBeenStopped:
    if not self.loopFuture.isNil:
      await self.loopFuture.cancelAndWait()
      self.loopFuture = nil
    if not self.session.isNil:
      await self.session.closeWait()
      self.session = nil
    self.bitswapTopDebtorRatios = initHashSet[string]()
    self.bitswapTopDebtorBytes = initHashSet[string]()
  hasBeenStopped

{.pop.}
