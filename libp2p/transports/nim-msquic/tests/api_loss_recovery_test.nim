import std/[strutils, times, unittest]

import chronos
import ../api/api_impl
import ../api/event_model
import ../congestion/common
import ../core/packet_model
import ../protocol/protocol_core as proto

proc nowUs(): uint64 =
  uint64(epochTime() * 1_000_000.0)

proc withConnection(body: proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC)) =
  var apiPtr: pointer
  check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
  let api = cast[ptr QuicApiTable](apiPtr)
  var registration: HQUIC
  check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
  var connection: HQUIC
  check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
  check seedConnectionIdsForTest(connection, @[0x01'u8, 0x02'u8], @[0xAA'u8, 0xBB'u8])
  try:
    body(api, registration, connection)
  finally:
    api.ConnectionClose(connection)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

suite "MsQuic ACK-driven loss recovery":
  test "packet threshold loss prunes older packets and schedules loss time":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, base, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 3'u64, base, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 4'u64, base - 200_000'u64, 1200'u16)

      check applyAckForTest(connection, packet_model.ceOneRtt, 4'u64)

      var count = 0'u32
      var lossTimeUs = 0'u64
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionLossTimeForTest(connection, packet_model.ceOneRtt, lossTimeUs)
      check count == 2'u32
      check lossTimeUs > base
    )

  test "time threshold loss marks stale packet without packet-threshold gap":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 8'u64, base - 50_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 10'u64, base - 10_000'u64, 1200'u16)

      check applyAckForTest(connection, packet_model.ceOneRtt, 10'u64)

      var count = 0'u32
      var lossTimeUs = high(uint64)
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionLossTimeForTest(connection, packet_model.ceOneRtt, lossTimeUs)
      check count == 0'u32
      check lossTimeUs == 0'u64
    )

  test "persistent congestion collapses congestion window to minimum":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, base - 800_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base - 450_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 3'u64, base - 100_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 4'u64, base - 80_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 5'u64, base - 60_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 6'u64, base - 40_000'u64, 1200'u16)

      check applyAckForTest(connection, packet_model.ceOneRtt, 6'u64)

      var windowBytes = 0'u64
      check getConnectionCongestionWindow(connection, windowBytes)
      check windowBytes == 4'u64 * 1200'u64
    )

  test "loss detection timer prefers earliest packet-space loss time":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs() + 200_000'u64
      check setConnectionLossTimeForTest(connection, packet_model.ceHandshake, base + 50_000'u64)
      check setConnectionLossTimeForTest(connection, packet_model.ceOneRtt, base + 90_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      var timerUs = 0'u64
      var epoch = packet_model.ceOneRtt
      var dueToLossTime = false
      check getConnectionLossDetectionTimerForTest(connection, timerUs)
      check getConnectionLossDetectionEpochForTest(connection, epoch, dueToLossTime)
      check timerUs == base + 50_000'u64
      check epoch == packet_model.ceHandshake
      check dueToLossTime
    )

  test "loss detection timer falls back to earliest packet-space PTO":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 200_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base - 40_000'u64, 1200'u16)

      var timerUs = 0'u64
      var epoch = packet_model.ceInitial
      var dueToLossTime = true
      check getConnectionLossDetectionTimerForTest(connection, timerUs)
      check getConnectionLossDetectionEpochForTest(connection, epoch, dueToLossTime)
      check timerUs == (base - 200_000'u64) + 300_000'u64
      check epoch == packet_model.ceInitial
      check not dueToLossTime
    )

  test "packet-space PTO uses newest sent ack-eliciting packet, not insertion order":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let olderSentUs = nowUs() - 300_000'u64
      let newerSentUs = olderSentUs + 180_000'u64
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 9'u64, newerSentUs, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 10'u64, olderSentUs, 1200'u16)

      var timerUs = 0'u64
      var epoch = packet_model.ceOneRtt
      var dueToLossTime = true
      check getConnectionLossDetectionTimerForTest(connection, timerUs)
      check getConnectionLossDetectionEpochForTest(connection, epoch, dueToLossTime)
      check epoch == packet_model.ceInitial
      check timerUs == newerSentUs + 300_000'u64
      check not dueToLossTime
    )

  test "one-rtt PTO includes peer ack delay while handshake PTO does not":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check setConnectionHandshakeStateForTest(connection, true)
      check setConnectionPeerMaxAckDelayForTest(connection, 25'u64)
      check seedSentPacketForTest(connection, packet_model.ceHandshake, 1'u64, base - 10_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base - 10_000'u64, 1200'u16)

      var timerUs = 0'u64
      var epoch = packet_model.ceInitial
      var dueToLossTime = true
      check getConnectionLossDetectionTimerForTest(connection, timerUs)
      check getConnectionLossDetectionEpochForTest(connection, epoch, dueToLossTime)
      check epoch == packet_model.ceHandshake
      check timerUs == (base - 10_000'u64) + 300_000'u64
      check not dueToLossTime
    )

  test "one-rtt PTO ignores peer ack delay before handshake is complete":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check setConnectionPeerMaxAckDelayForTest(connection, 25'u64)
      check setConnectionHandshakeStateForTest(connection, false)

      var timeoutUs = 0'u64
      check getConnectionCurrentProbeTimeoutForTest(connection, packet_model.ceOneRtt, timeoutUs)
      check timeoutUs == 300_000'u64

      check setConnectionHandshakeStateForTest(connection, true)
      check getConnectionCurrentProbeTimeoutForTest(connection, packet_model.ceOneRtt, timeoutUs)
      check timeoutUs == 325_000'u64
    )

  test "one-rtt ACK ignores peer ack delay before handshake is complete":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let sentUs = nowUs() - 200_000'u64
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 100_000'u64, 50_000'u64)
      check setConnectionPeerMaxAckDelayForTest(connection, 25'u64)
      check setConnectionPeerAckDelayExponentForTest(connection, 0'u8)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, sentUs, 1200'u16)

      check setConnectionHandshakeStateForTest(connection, false)
      check applyAckForTest(connection, packet_model.ceOneRtt, 1'u64, 25_000'u64)
      var latestRttUs = 0'u64
      var smoothedRttUs = 0'u64
      var minRttUs = 0'u64
      var rttVarianceUs = 0'u64
      check getConnectionRttStats(connection, latestRttUs, smoothedRttUs, minRttUs, rttVarianceUs)
      let preHandshakeRttUs = latestRttUs
      check preHandshakeRttUs >= 180_000'u64

      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 100_000'u64, 50_000'u64)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, sentUs, 1200'u16)
      check setConnectionHandshakeStateForTest(connection, true)
      check applyAckForTest(connection, packet_model.ceOneRtt, 2'u64, 25_000'u64)
      check getConnectionRttStats(connection, latestRttUs, smoothedRttUs, minRttUs, rttVarianceUs)
      check latestRttUs + 20_000'u64 < preHandshakeRttUs
    )

  test "one-rtt ACK uses default max ack delay clamp when peer transport parameter is absent":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let sampleAgeUs = 250_000'u64
      let encodedAckDelayUs = 40_000'u64
      check setConnectionHandshakeStateForTest(connection, true)
      check setConnectionPeerAckDelayExponentForTest(connection, 0'u8)

      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 100_000'u64, 50_000'u64)
      check setConnectionPeerMaxAckDelayForTest(connection, 0'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        nowUs() - sampleAgeUs,
        1200'u16
      )
      check applyAckForTest(connection, packet_model.ceOneRtt, 1'u64, encodedAckDelayUs)
      var clampedLatestRttUs = 0'u64
      var smoothedRttUs = 0'u64
      var minRttUs = 0'u64
      var rttVarianceUs = 0'u64
      check getConnectionRttStats(
        connection,
        clampedLatestRttUs,
        smoothedRttUs,
        minRttUs,
        rttVarianceUs
      )

      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 100_000'u64, 50_000'u64)
      check setConnectionPeerMaxAckDelayForTest(connection, 100'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        2'u64,
        nowUs() - sampleAgeUs,
        1200'u16
      )
      check applyAckForTest(connection, packet_model.ceOneRtt, 2'u64, encodedAckDelayUs)
      var fullDelayLatestRttUs = 0'u64
      check getConnectionRttStats(
        connection,
        fullDelayLatestRttUs,
        smoothedRttUs,
        minRttUs,
        rttVarianceUs
      )
      check clampedLatestRttUs > fullDelayLatestRttUs + 10_000'u64
    )

  test "one-rtt ACK applies peer ack delay exponent after handshake is complete":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let sampleAgeUs = 250_000'u64
      let encodedAckDelayUnits = 10_000'u64
      check setConnectionHandshakeStateForTest(connection, true)
      check setConnectionPeerMaxAckDelayForTest(connection, 100'u64)

      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 100_000'u64, 50_000'u64)
      check setConnectionPeerAckDelayExponentForTest(connection, 0'u8)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        nowUs() - sampleAgeUs,
        1200'u16
      )
      check applyAckForTest(connection, packet_model.ceOneRtt, 1'u64, encodedAckDelayUnits)
      var exponentZeroLatestRttUs = 0'u64
      var smoothedRttUs = 0'u64
      var minRttUs = 0'u64
      var rttVarianceUs = 0'u64
      check getConnectionRttStats(
        connection,
        exponentZeroLatestRttUs,
        smoothedRttUs,
        minRttUs,
        rttVarianceUs
      )

      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 100_000'u64, 50_000'u64)
      check setConnectionPeerAckDelayExponentForTest(connection, 2'u8)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        2'u64,
        nowUs() - sampleAgeUs,
        1200'u16
      )
      check applyAckForTest(connection, packet_model.ceOneRtt, 2'u64, encodedAckDelayUnits)
      var exponentShiftedLatestRttUs = 0'u64
      check getConnectionRttStats(
        connection,
        exponentShiftedLatestRttUs,
        smoothedRttUs,
        minRttUs,
        rttVarianceUs
      )
      check exponentZeroLatestRttUs > exponentShiftedLatestRttUs + 20_000'u64
    )

  test "bytes in flight are tracked independently per packet space":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 60_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base - 30_000'u64, 900'u16)

      var packets = 0'u32
      var bytes = 0'u64
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check packets == 1'u32
      check bytes == 1200'u64
      check getConnectionLossFlightForTest(connection, packet_model.ceOneRtt, packets, bytes)
      check packets == 1'u32
      check bytes == 900'u64

      check applyAckForTest(connection, packet_model.ceInitial, 1'u64)
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check packets == 0'u32
      check bytes == 0'u64
      check getConnectionLossFlightForTest(connection, packet_model.ceOneRtt, packets, bytes)
      check packets == 1'u32
      check bytes == 900'u64
    )

  test "loss recovery tick only fires earliest scheduled epoch":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 500_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base - 100_000'u64, 1200'u16)
      check setConnectionLossTimeForTest(connection, packet_model.ceInitial, base - 10_000'u64)
      check setConnectionLossTimeForTest(connection, packet_model.ceOneRtt, base + 50_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var count = 0'u32
      var initialLossTime = 0'u64
      var oneRttLossTime = 0'u64
      var epoch = packet_model.ceOneRtt
      var dueToLossTime = false
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionLossTimeForTest(connection, packet_model.ceInitial, initialLossTime)
      check getConnectionLossTimeForTest(connection, packet_model.ceOneRtt, oneRttLossTime)
      check getConnectionLossDetectionEpochForTest(connection, epoch, dueToLossTime)
      check count == 1'u32
      check initialLossTime == 0'u64
      check oneRttLossTime == base + 50_000'u64
      check epoch == packet_model.ceOneRtt
      check dueToLossTime
    )

  test "ack state tracks the actual largest acked packet send snapshot":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let olderSentUs = nowUs() - 300_000'u64
      let newerSentUs = olderSentUs + 100_000'u64
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, olderSentUs, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, newerSentUs, 1400'u16)

      check applyAckForTest(connection, packet_model.ceOneRtt, 1'u64)

      var ackedSentUs = 0'u64
      var totalBytesSentAtAck = 0'u64
      check getConnectionLossAckStateForTest(connection, ackedSentUs, totalBytesSentAtAck)
      check ackedSentUs == olderSentUs
      check totalBytesSentAtAck == 1200'u64
    )

  test "loss accounting does not confuse duplicate packet numbers across epochs":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceInitial, 7'u64, base - 500_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 7'u64, base - 50_000'u64, 900'u16)
      check setConnectionLossTimeForTest(connection, packet_model.ceInitial, base - 10_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var packets = 0'u32
      var bytes = 0'u64
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check packets == 0'u32
      check bytes == 0'u64
      check getConnectionLossFlightForTest(connection, packet_model.ceOneRtt, packets, bytes)
      check packets == 1'u32
      check bytes == 900'u64
    )

  test "pto backoff is isolated per packet space":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 500_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 2'u64, base - 50_000'u64, 1200'u16)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var aggregateProbe = 0'u16
      var initialProbe = 0'u16
      var oneRttProbe = 0'u16
      var packets = 0'u32
      var bytes = 0'u64
      check getConnectionProbeCount(connection, aggregateProbe)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, initialProbe)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceOneRtt, oneRttProbe)
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check aggregateProbe == 1'u16
      check initialProbe == 1'u16
      check oneRttProbe == 0'u16
      check packets == 1'u32
      check bytes == 1200'u64
    )

  test "ack resets PTO backoff only for the acknowledged packet space":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check seedConnectionIdsForTest(connection, @[0x01'u8, 0x02'u8], @[0xAA'u8, 0xBB'u8], false)
      let base = nowUs()
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 20_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, base - 20_000'u64, 1200'u16)
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceInitial, 2'u16)
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceOneRtt, 3'u16)

      check applyAckForTest(connection, packet_model.ceInitial, 1'u64)

      var initialProbe = 0'u16
      var oneRttProbe = 0'u16
      var aggregateProbe = 0'u16
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, initialProbe)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceOneRtt, oneRttProbe)
      check getConnectionProbeCount(connection, aggregateProbe)
      check initialProbe == 0'u16
      check oneRttProbe == 3'u16
      check aggregateProbe == 3'u16
    )

  test "client initial ack before handshake complete does not reset PTO backoff":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check setConnectionHandshakeStateForTest(connection, false)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 20_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, base - 20_000'u64, 1200'u16)
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceInitial, 2'u16)
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceOneRtt, 3'u16)

      check applyAckForTest(connection, packet_model.ceInitial, 1'u64)

      var initialProbe = 0'u16
      var oneRttProbe = 0'u16
      var aggregateProbe = 0'u16
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, initialProbe)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceOneRtt, oneRttProbe)
      check getConnectionProbeCount(connection, aggregateProbe)
      check initialProbe == 2'u16
      check oneRttProbe == 3'u16
      check aggregateProbe == 3'u16
    )

  test "client initial ack after peer address validation resets initial PTO backoff":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check setConnectionHandshakeStateForTest(connection, false)
      check setConnectionPeerAddressValidatedForTest(connection, true)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 20_000'u64, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceOneRtt, 1'u64, base - 20_000'u64, 1200'u16)
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceInitial, 2'u16)
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceOneRtt, 3'u16)

      check applyAckForTest(connection, packet_model.ceInitial, 1'u64)

      var initialProbe = 0'u16
      var oneRttProbe = 0'u16
      var aggregateProbe = 0'u16
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, initialProbe)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceOneRtt, oneRttProbe)
      check getConnectionProbeCount(connection, aggregateProbe)
      check initialProbe == 0'u16
      check oneRttProbe == 3'u16
      check aggregateProbe == 3'u16
    )

  test "ack of non-ack-eliciting packets does not reset PTO backoff":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(
        connection,
        packet_model.ceInitial,
        1'u64,
        base - 20_000'u64,
        64'u16,
        ackEliciting = false
      )
      check setConnectionProbeCountForEpochForTest(connection, packet_model.ceInitial, 2'u16)

      check applyAckForTest(connection, packet_model.ceInitial, 1'u64)

      var initialProbe = 0'u16
      var aggregateProbe = 0'u16
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, initialProbe)
      check getConnectionProbeCount(connection, aggregateProbe)
      check initialProbe == 2'u16
      check aggregateProbe == 2'u16
    )

  test "ack of non-ack-eliciting packets does not update RTT or acked send snapshot":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check seedSentPacketForTest(
        connection,
        packet_model.ceInitial,
        1'u64,
        base - 20_000'u64,
        64'u16,
        ackEliciting = false
      )

      check applyAckForTest(connection, packet_model.ceInitial, 1'u64)

      var latestRttUs = 0'u64
      var smoothedRttUs = 0'u64
      var minRttUs = 0'u64
      var rttVarianceUs = 0'u64
      var ackedSentUs = 0'u64
      var totalBytesSentAtAck = 0'u64
      check getConnectionRttStats(connection, latestRttUs, smoothedRttUs, minRttUs, rttVarianceUs)
      check getConnectionLossAckStateForTest(connection, ackedSentUs, totalBytesSentAtAck)
      check latestRttUs == 0'u64
      check smoothedRttUs == 0'u64
      check minRttUs == 0'u64
      check rttVarianceUs == 0'u64
      check ackedSentUs == 0'u64
      check totalBytesSentAtAck == 0'u64
    )

  test "mixed ACK uses the newest acknowledged ack-eliciting packet for RTT sampling":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let ackElicitingSentUs = nowUs() - 300_000'u64
      let ackOnlySentUs = ackElicitingSentUs + 150_000'u64
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, ackElicitingSentUs, 1200'u16)
      check seedSentPacketForTest(
        connection,
        packet_model.ceInitial,
        2'u64,
        ackOnlySentUs,
        64'u16,
        ackEliciting = false
      )

      check applyAckRangesForTest(connection, packet_model.ceInitial, @[(1'u64, 1'u64), (2'u64, 2'u64)])

      var latestRttUs = 0'u64
      var smoothedRttUs = 0'u64
      var minRttUs = 0'u64
      var rttVarianceUs = 0'u64
      var ackedSentUs = 0'u64
      var totalBytesSentAtAck = 0'u64
      check getConnectionRttStats(connection, latestRttUs, smoothedRttUs, minRttUs, rttVarianceUs)
      check getConnectionLossAckStateForTest(connection, ackedSentUs, totalBytesSentAtAck)
      check latestRttUs > 0'u64
      check smoothedRttUs > 0'u64
      check minRttUs > 0'u64
      check rttVarianceUs > 0'u64
      check ackedSentUs == ackElicitingSentUs
      check totalBytesSentAtAck == 1200'u64
    )

  test "ACK sampling prefers newest sent ack-eliciting packet over largest packet number":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let olderSentUs = nowUs() - 450_000'u64
      let newerSentUs = olderSentUs + 180_000'u64
      check seedSentPacketForTest(connection, packet_model.ceInitial, 9'u64, olderSentUs, 1200'u16)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 5'u64, newerSentUs, 900'u16)

      check applyAckRangesForTest(connection, packet_model.ceInitial, @[(5'u64, 5'u64), (9'u64, 9'u64)])

      var latestRttUs = 0'u64
      var smoothedRttUs = 0'u64
      var minRttUs = 0'u64
      var rttVarianceUs = 0'u64
      var ackedSentUs = 0'u64
      var totalBytesSentAtAck = 0'u64
      check getConnectionRttStats(connection, latestRttUs, smoothedRttUs, minRttUs, rttVarianceUs)
      check getConnectionLossAckStateForTest(connection, ackedSentUs, totalBytesSentAtAck)
      check latestRttUs > 0'u64
      check smoothedRttUs > 0'u64
      check minRttUs > 0'u64
      check rttVarianceUs > 0'u64
      check ackedSentUs == newerSentUs
      check totalBytesSentAtAck == 2100'u64
    )

  test "BBR preserves app-limited flag when the largest acked ack-eliciting packet was app-limited":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check setConnectionCongestionAlgorithmForTest(connection, caBbr)
      check setConnectionBbrAppLimitedForTest(connection, true)
      let base = nowUs()
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 30_000'u64,
        1200'u16,
        ackEliciting = true,
        appLimited = true
      )
      check applyAckForTest(connection, packet_model.ceOneRtt, 1'u64)

      var appLimited = false
      check getConnectionBbrAppLimitedForTest(connection, appLimited)
      check appLimited
    )

  test "BBR clears app-limited flag when the largest acked ack-eliciting packet was not app-limited":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check setConnectionCongestionAlgorithmForTest(connection, caBbr)
      check setConnectionBbrAppLimitedForTest(connection, true)
      let base = nowUs()
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 30_000'u64,
        1200'u16,
        ackEliciting = true,
        appLimited = false
      )
      check applyAckForTest(connection, packet_model.ceOneRtt, 1'u64)

      var appLimited = true
      check getConnectionBbrAppLimitedForTest(connection, appLimited)
      check not appLimited
    )

  test "pto timeout sends a pair of real probe packets when transport is ready":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      check prepareConnectionPacketSendForTest(connection, packet_model.ceInitial)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 500_000'u64, 1200'u16)

      var count = 0'u32
      var packets = 0'u32
      var bytes = 0'u64
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check packets == 1'u32
      check bytes == 1200'u64

      check runConnectionLossRecoveryTick(connection)

      var aggregateProbe = 0'u16
      var initialProbe = 0'u16
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionProbeCount(connection, aggregateProbe)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, initialProbe)
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check count == 3'u32
      check aggregateProbe == 1'u16
      check initialProbe == 1'u16
      check packets == 3'u32
      check bytes > 1200'u64
    )

  test "pto timeout reuses outstanding initial frame payload for probe packets":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let cryptoPayload = @[0x06'u8, 0x00, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF]
      check prepareConnectionPacketSendForTest(connection, packet_model.ceInitial)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceInitial,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkCrypto,
        framePayload = cryptoPayload
      )

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 3'u32
      check frameKind == sfkCrypto
      check payloadLen == uint32(cryptoPayload.len)
      check payload == cryptoPayload
    )

  test "pto timeout reuses original remote for initial crypto probes":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let cryptoPayload = @[0x06'u8, 0x00, 0x00, 0x04, 0xA1, 0xB2]
      let targetRemote = initTAddress("10.0.2.22", Port(4622))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceInitial)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceInitial,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkCrypto,
        framePayload = cryptoPayload,
        targetRemote = targetRemote
      )

      check runConnectionLossRecoveryTick(connection)

      var frameKind = sfkAck
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check frameKind == sfkCrypto
      check payload == cryptoPayload
      check port == 4622'u16
      check host.contains("10.0.2.22")
    )

  test "pto timeout reuses original remote for handshake crypto probes":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let cryptoPayload = @[0x06'u8, 0x00, 0x02, 0xCA, 0xFE]
      let targetRemote = initTAddress("10.0.2.33", Port(4633))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceHandshake)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceHandshake,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkCrypto,
        framePayload = cryptoPayload,
        targetRemote = targetRemote
      )

      check runConnectionLossRecoveryTick(connection)

      var frameKind = sfkAck
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check frameKind == sfkCrypto
      check payload == cryptoPayload
      check port == 4633'u16
      check host.contains("10.0.2.33")
    )

  test "pto timeout uses ping probes instead of retransmitting zero-rtt datagram payload":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let datagramPayload = proto.encodeDatagramFrame(@[0xAA'u8, 0xBB, 0xCC, 0xDD, 0xEE], true)
      let pingPayload = @[0x01'u8]
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceZeroRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkDatagram,
        framePayload = datagramPayload
      )

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 3'u32
      check frameKind == sfkPing
      check payloadLen == uint32(pingPayload.len)
      check payload == pingPayload
    )

  test "pto timeout preserves original remote for zero-rtt ping probes":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let datagramPayload = proto.encodeDatagramFrame(@[0xAA'u8, 0xBB, 0xCC], true)
      let pingPayload = @[0x01'u8]
      let targetRemote = initTAddress("10.0.2.66", Port(4766))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceZeroRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkDatagram,
        framePayload = datagramPayload,
        targetRemote = targetRemote
      )

      check runConnectionLossRecoveryTick(connection)

      var frameKind = sfkAck
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check frameKind == sfkPing
      check payload == pingPayload
      check port == 4766'u16
      check host.contains("10.0.2.66")
    )

  test "zero-rtt ack is queued until handshake completes and then sent in one-rtt":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.77"
      let remotePort = 4777'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0xA0'u8, 0xB1, 0xC2], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check receiveZeroRttPayloadForTest(connection, 7'u64, datagramPayload, remoteHost, remotePort)

      var count = 0'u32
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 0'u32

      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var frameKind = sfkDatagram
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check count == 1'u32
      check frameKind == sfkAck
      check port == remotePort
      check host.contains(remoteHost)
      var pos = 0
      let ack = proto.parseAckFrame(payload, pos)
      check ack.largestAcked == 7'u64
    )

  test "completeServerHandshake flushes queued zero-rtt ack to original remote":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let originalRemoteHost = "10.0.2.177"
      let originalRemotePort = 5177'u16
      let migratedRemoteHost = "10.0.2.188"
      let migratedRemotePort = 5188'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0xD7'u8, 0xE8, 0xF9], true)

      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check receiveZeroRttPayloadForTest(
        connection,
        17'u64,
        datagramPayload,
        originalRemoteHost,
        originalRemotePort
      )

      var count = 0'u32
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 0'u32

      check setConnectionRemoteAddressForTest(connection, migratedRemoteHost, migratedRemotePort)
      check completeServerHandshakeForTest(connection)

      check getConnectionSentPacketCountForTest(connection, count)
      check count >= 2'u32

      var foundAck = false
      for idx in 0'u32 ..< count:
        var frameKind = sfkDatagram
        var payload: seq[byte] = @[]
        var host = ""
        var port = 0'u16
        check getConnectionSentFrameKindAtForTest(connection, idx, frameKind)
        check getConnectionSentFramePayloadAtForTest(connection, idx, payload)
        check getConnectionSentTargetRemoteAtForTest(connection, idx, host, port)
        if frameKind == sfkAck:
          foundAck = true
          check port == originalRemotePort
          check host.contains(originalRemoteHost)
          var pos = 0
          let ack = proto.parseAckFrame(payload, pos)
          check ack.largestAcked == 17'u64
      check foundAck
    )

  test "pending one-rtt ack is consumed after flush and not resent without new packets":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.88"
      let remotePort = 4888'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0xD0'u8, 0xE1, 0xF2], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check receiveZeroRttPayloadForTest(connection, 9'u64, datagramPayload, remoteHost, remotePort)

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 1'u32
      check ackPacketsToAck == 1'u16
      check not alreadyWritten

      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var count = 0'u32
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 0'u32
      check ackPacketsToAck == 0'u16
      check alreadyWritten

      check not flushPendingOneRttAckForTest(connection, remoteHost, remotePort)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check not sendAckForTest(connection, packet_model.ceOneRtt, remoteHost, remotePort)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
    )

  test "duplicate zero-rtt datagrams are not delivered twice":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check MsQuicEnableDatagramReceiveShim(connection, BOOLEAN(1)) == QUIC_STATUS_SUCCESS

      let remoteHost = "10.0.2.101"
      let remotePort = 4101'u16
      let datagramBytes = @[0xC1'u8, 0xD2, 0xE3]
      let datagramPayload = proto.encodeDatagramFrame(datagramBytes, true)
      var datagramReceives = 0
      registerConnectionEventHandler(connection, proc(ev: ConnectionEvent) =
        if ev.kind == ceDatagramReceived and ev.datagramPayload == datagramBytes:
          inc datagramReceives
      )

      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check receiveZeroRttPayloadForTest(connection, 21'u64, datagramPayload, remoteHost, remotePort)
      check receiveZeroRttPayloadForTest(connection, 21'u64, datagramPayload, remoteHost, remotePort)

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check datagramReceives == 1
      check pendingAckRanges == 1'u32
      check ackPacketsToAck == 1'u16
      check not alreadyWritten
    )

  test "duplicate one-rtt datagrams are not delivered twice":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check MsQuicEnableDatagramReceiveShim(connection, BOOLEAN(1)) == QUIC_STATUS_SUCCESS

      let remoteHost = "10.0.2.102"
      let remotePort = 4102'u16
      let datagramBytes = @[0x91'u8, 0xA2, 0xB3]
      let datagramPayload = proto.encodeDatagramFrame(datagramBytes, true)
      var datagramReceives = 0
      registerConnectionEventHandler(connection, proc(ev: ConnectionEvent) =
        if ev.kind == ceDatagramReceived and ev.datagramPayload == datagramBytes:
          inc datagramReceives
      )

      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)
      check receiveOneRttPayloadForTest(connection, 22'u64, datagramPayload, remoteHost, remotePort)
      check receiveOneRttPayloadForTest(connection, 22'u64, datagramPayload, remoteHost, remotePort)
      check datagramReceives == 1
    )

  test "pending one-rtt ack encodes sparse ranges instead of cumulative ack":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.99"
      let remotePort = 4999'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0xA1'u8, 0xB2, 0xC3], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check receiveZeroRttPayloadForTest(connection, 1'u64, datagramPayload, remoteHost, remotePort)
      check receiveZeroRttPayloadForTest(connection, 3'u64, datagramPayload, remoteHost, remotePort)

      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var frameKind = sfkDatagram
      var payload: seq[byte] = @[]
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check frameKind == sfkAck
      var pos = 0
      let ack = proto.parseAckFrame(payload, pos)
      check ack.largestAcked == 3'u64
      check ack.ranges.len == 2
      check ack.ranges[0].smallest == 3'u64
      check ack.ranges[0].largest == 3'u64
      check ack.ranges[1].smallest == 1'u64
      check ack.ranges[1].largest == 1'u64
    )

  test "pending one-rtt ack encodes local ack delay while handshake ack stays zero":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.77"
      let remotePort = 4777'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0xD1'u8, 0xE2, 0xF3], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check receiveZeroRttPayloadForTest(connection, 11'u64, datagramPayload, remoteHost, remotePort)
      let ackRecvTimeUs = nowUs() - 16_000'u64
      check setConnectionLargestAckRecvTimeForTest(connection, ackRecvTimeUs)
      check setConnectionLocalAckDelayExponentForTest(connection, 3'u64)
      check setConnectionLocalMaxAckDelayForTest(connection, 25'u64)

      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var frameKind = sfkDatagram
      var payload: seq[byte] = @[]
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check frameKind == sfkAck
      var pos = 0
      let oneRttAck = proto.parseAckFrame(payload, pos)
      check oneRttAck.delay > 0'u64

      check prepareConnectionPacketSendForTest(connection, packet_model.ceHandshake)
      check setConnectionHandshakeStateForTest(connection, false)
      check markPendingAckForTest(connection, 12'u64, nowUs() - 16_000'u64, packet_model.ceHandshake)
      var handshakePayload: seq[byte] = @[]
      var handshakeFrameKind = sfkDatagram
      check sendAckForTest(connection, packet_model.ceHandshake, remoteHost, remotePort)
      check getConnectionLastSentFrameKindForTest(connection, handshakeFrameKind)
      check getConnectionLastSentFramePayloadForTest(connection, handshakePayload)
      check handshakeFrameKind == sfkAck
      pos = 0
      let handshakeAck = proto.parseAckFrame(handshakePayload, pos)
      check handshakeAck.delay == 0'u64
    )

  test "first in-order one-rtt packet delays ack until threshold is reached":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.55"
      let remotePort = 4555'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0x11'u8, 0x22, 0x33], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)

      var sentCount = 0'u32
      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 0'u32

      check receiveOneRttPayloadForTest(connection, 30'u64, datagramPayload, remoteHost, remotePort)

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      var ackDeadlineUs = 0'u64
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check getConnectionPendingAckDeadlineForTest(connection, ackDeadlineUs)
      check pendingAckRanges == 1'u32
      check ackPacketsToAck == 1'u16
      check not alreadyWritten
      check ackDeadlineUs > 0'u64

      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 1'u32
      var frameKind = sfkAck
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check frameKind != sfkAck

      check receiveOneRttPayloadForTest(connection, 31'u64, datagramPayload, remoteHost, remotePort)
      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 2'u32
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check frameKind == sfkAck

      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 0'u32
      check ackPacketsToAck == 0'u16
      check alreadyWritten
      check getConnectionPendingAckDeadlineForTest(connection, ackDeadlineUs)
      check ackDeadlineUs == 0'u64
    )

  test "gap one-rtt packet after consumed ack flushes ack immediately as reordered":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.57"
      let remotePort = 4557'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0x41'u8, 0x52, 0x63], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)

      check receiveOneRttPayloadForTest(connection, 40'u64, datagramPayload, remoteHost, remotePort)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 0'u32
      check ackPacketsToAck == 0'u16
      check alreadyWritten

      var sentCount = 0'u32
      check getConnectionSentPacketCountForTest(connection, sentCount)
      let sentCountAfterFlush = sentCount

      check receiveOneRttPayloadForTest(connection, 42'u64, datagramPayload, remoteHost, remotePort)

      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount > sentCountAfterFlush
      var frameKind = sfkDatagram
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check frameKind == sfkAck

      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 0'u32
      check ackPacketsToAck == 0'u16
      check alreadyWritten
    )

  test "loss recovery tick flushes delayed one-rtt ack after deadline":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.56"
      let remotePort = 4556'u16
      let recvTimeUs = nowUs() - 40_000'u64
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)
      check markDelayedPendingAckForTest(connection, 32'u64, recvTimeUs, 10'u32)

      var sentCount = 0'u32
      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 0'u32

      var ackDeadlineUs = 0'u64
      check getConnectionPendingAckDeadlineForTest(connection, ackDeadlineUs)
      check ackDeadlineUs > 0'u64
      check ackDeadlineUs <= nowUs()

      check runConnectionLossRecoveryTick(connection)
      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 1'u32

      var frameKind = sfkDatagram
      var payload: seq[byte] = @[]
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check frameKind == sfkAck
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      var pos = 0
      let ack = proto.parseAckFrame(payload, pos)
      check ack.largestAcked == 32'u64

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 0'u32
      check ackPacketsToAck == 0'u16
      check alreadyWritten
      check getConnectionPendingAckDeadlineForTest(connection, ackDeadlineUs)
      check ackDeadlineUs == 0'u64
    )

  test "loss recovery tick flushes delayed one-rtt ack to original remote after active remote changes":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let originalHost = "10.0.2.156"
      let originalPort = 5156'u16
      let migratedHost = "10.0.2.166"
      let migratedPort = 5166'u16
      let recvTimeUs = nowUs() - 40_000'u64
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)
      check markDelayedPendingAckForTest(connection, 33'u64, recvTimeUs, 10'u32)
      check setConnectionPendingAckRemoteForTest(connection, packet_model.ceOneRtt, originalHost, originalPort)
      check setConnectionRemoteAddressForTest(connection, migratedHost, migratedPort)

      check runConnectionLossRecoveryTick(connection)

      var frameKind = sfkDatagram
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check frameKind == sfkAck
      check port == originalPort
      check host.contains(originalHost)
    )

  test "one-rtt remote change flushes pending delayed ack before tracking new path":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let originalHost = "10.0.2.171"
      let originalPort = 5171'u16
      let migratedHost = "10.0.2.172"
      let migratedPort = 5172'u16
      let recvTimeUs = nowUs() - 5_000'u64
      let datagramPayload = proto.encodeDatagramFrame(@[0x61'u8, 0x72, 0x83], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)
      check markDelayedPendingAckForTest(connection, 50'u64, recvTimeUs, 25'u32)
      check setConnectionPendingAckRemoteForTest(connection, packet_model.ceOneRtt, originalHost, originalPort)

      var sentCount = 0'u32
      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 0'u32

      check receiveOneRttPayloadForTest(connection, 51'u64, datagramPayload, migratedHost, migratedPort)

      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 2'u32

      var frameKind = sfkDatagram
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionSentFrameKindAtForTest(connection, 0'u32, frameKind)
      check frameKind == sfkPathChallenge
      check getConnectionSentTargetRemoteAtForTest(connection, 0'u32, host, port)
      check port == migratedPort
      check host.contains(migratedHost)

      check getConnectionSentFrameKindAtForTest(connection, 1'u32, frameKind)
      check getConnectionSentTargetRemoteAtForTest(connection, 1'u32, host, port)
      check getConnectionSentFramePayloadAtForTest(connection, 1'u32, payload)
      check frameKind == sfkAck
      check port == originalPort
      check host.contains(originalHost)
      var pos = 0
      let ack = proto.parseAckFrame(payload, pos)
      check ack.largestAcked == 50'u64

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      var ackDeadlineUs = 0'u64
      var pendingHost = ""
      var pendingPort = 0'u16
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check getConnectionPendingAckDeadlineForTest(connection, ackDeadlineUs)
      check getConnectionPendingAckRemoteForTest(connection, packet_model.ceOneRtt, pendingHost, pendingPort)
      check pendingAckRanges == 1'u32
      check ackPacketsToAck == 1'u16
      check not alreadyWritten
      check ackDeadlineUs > 0'u64
      check pendingPort == migratedPort
      check pendingHost.contains(migratedHost)
    )

  test "one-rtt ack send before handshake complete preserves pending ack state":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.91"
      let remotePort = 4991'u16
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check markPendingAckForTest(connection, 44'u64, nowUs() - 2_000'u64)

      var pendingAckRanges = 0'u32
      var ackPacketsToAck = 0'u16
      var alreadyWritten = false
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 1'u32
      check ackPacketsToAck == 1'u16
      check not alreadyWritten

      check not sendAckForTest(connection, packet_model.ceOneRtt, remoteHost, remotePort)
      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 1'u32
      check ackPacketsToAck == 1'u16
      check not alreadyWritten

      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var sentCount = 0'u32
      check getConnectionSentPacketCountForTest(connection, sentCount)
      check sentCount == 1'u32
      var frameKind = sfkDatagram
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check frameKind == sfkAck

      check getConnectionPendingAckStateForTest(
        connection,
        pendingAckRanges,
        ackPacketsToAck,
        alreadyWritten
      )
      check pendingAckRanges == 0'u32
      check ackPacketsToAck == 0'u16
      check alreadyWritten
    )

  test "out-of-order pending ack merges adjacent ranges into one span":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.66"
      let remotePort = 4666'u16
      let datagramPayload = proto.encodeDatagramFrame(@[0x91'u8, 0x82, 0x73], true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionHandshakeStateForTest(connection, false)
      check receiveZeroRttPayloadForTest(connection, 5'u64, datagramPayload, remoteHost, remotePort)
      check receiveZeroRttPayloadForTest(connection, 3'u64, datagramPayload, remoteHost, remotePort)
      check receiveZeroRttPayloadForTest(connection, 4'u64, datagramPayload, remoteHost, remotePort)

      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingOneRttAckForTest(connection, remoteHost, remotePort)

      var payload: seq[byte] = @[]
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      var pos = 0
      let ack = proto.parseAckFrame(payload, pos)
      check ack.largestAcked == 5'u64
      check ack.ranges.len == 1
      check ack.ranges[0].smallest == 3'u64
      check ack.ranges[0].largest == 5'u64
    )

  test "late smaller packet does not overwrite largest ack recv time used for ack delay":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let remoteHost = "10.0.2.55"
      let remotePort = 4555'u16
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)
      check setConnectionLocalAckDelayExponentForTest(connection, 0'u64)
      check setConnectionLocalMaxAckDelayForTest(connection, 50'u64)
      let largerRecvUs = nowUs() - 20_000'u64
      let smallerRecvUs = nowUs() - 5_000'u64
      check markPendingAckForTest(connection, 10'u64, largerRecvUs)
      check markPendingAckForTest(connection, 8'u64, smallerRecvUs)
      check sendAckForTest(connection, packet_model.ceOneRtt, remoteHost, remotePort)

      var payload: seq[byte] = @[]
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      var pos = 0
      let ack = proto.parseAckFrame(payload, pos)
      check ack.largestAcked == 10'u64
      check ack.delay >= 10_000'u64
    )

  test "pto timeout reuses outstanding one-rtt stream frame payload for probe packets":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let streamPayload = proto.encodeStreamFrame(0'u64, @[0x11'u8, 0x22, 0x33, 0x44], 0'u64, false)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = streamPayload
      )

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 3'u32
      check frameKind == sfkStream
      check payloadLen == uint32(streamPayload.len)
      check payload == streamPayload
    )

  test "pto timeout uses ping probes instead of retransmitting one-rtt datagram payload":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let datagramPayload = proto.encodeDatagramFrame(@[0x61'u8, 0x72, 0x83], true)
      let pingPayload = @[0x01'u8]
      let targetRemote = initTAddress("10.0.2.144", Port(4944))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkDatagram,
        framePayload = datagramPayload,
        targetRemote = targetRemote
      )

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 3'u32
      check frameKind == sfkPing
      check payloadLen == uint32(pingPayload.len)
      check payload == pingPayload
      check port == 4944'u16
      check host.contains("10.0.2.144")
    )

  test "pto timeout reuses original remote for one-rtt path-challenge probes":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let challengeData = [byte 1, 3, 5, 7, 9, 11, 13, 15]
      let challengePayload = proto.encodePathChallengeFrame(challengeData)
      let targetRemote = initTAddress("10.0.2.44", Port(4744))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkPathChallenge,
        framePayload = challengePayload,
        targetRemote = targetRemote
      )

      check runConnectionLossRecoveryTick(connection)

      var frameKind = sfkAck
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check frameKind == sfkPathChallenge
      check payload == challengePayload
      check port == 4744'u16
      check host.contains("10.0.2.44")
    )

  test "pto timeout reuses original remote for one-rtt path-response probes":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let responseData = [byte 15, 13, 11, 9, 7, 5, 3, 1]
      let responsePayload = proto.encodePathResponseFrame(responseData)
      let targetRemote = initTAddress("10.0.2.55", Port(4755))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkPathResponse,
        framePayload = responsePayload,
        targetRemote = targetRemote
      )

      check runConnectionLossRecoveryTick(connection)

      var frameKind = sfkAck
      var payload: seq[byte] = @[]
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check frameKind == sfkPathResponse
      check payload == responsePayload
      check port == 4755'u16
      check host.contains("10.0.2.55")
    )

  test "loss timer retransmits lost handshake crypto control frames":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let cryptoPayload = @[0x06'u8, 0x00, 0x01, 0xAA]
      let targetRemote = initTAddress("10.0.0.77", Port(4777))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceHandshake)
      check seedSentPacketForTest(
        connection,
        packet_model.ceHandshake,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkCrypto,
        framePayload = cryptoPayload,
        targetRemote = targetRemote
      )
      check setConnectionLossTimeForTest(connection, packet_model.ceHandshake, base - 10_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check frameKind == sfkCrypto
      check payloadLen == uint32(cryptoPayload.len)
      check payload == cryptoPayload
      check port == 4777'u16
      check host.contains("10.0.0.77")
    )

  test "ack-driven loss retransmits lost one-rtt handshake-done control frames":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let handshakeDonePayload = proto.encodeHandshakeDoneFrame()
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkHandshakeDone,
        framePayload = handshakeDonePayload
      )
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        4'u64,
        base - 10_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = proto.encodeStreamFrame(0'u64, @[0x44'u8], 0'u64, false)
      )

      check applyAckForTest(connection, packet_model.ceOneRtt, 4'u64)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check frameKind == sfkHandshakeDone
      check payloadLen == uint32(handshakeDonePayload.len)
      check payload == handshakeDonePayload
    )

  test "ack-driven loss retransmits lost one-rtt stream frames immediately":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let streamPayload = @[0x9A'u8, 0xBC, 0xDE]
      let streamFrame = proto.encodeStreamFrame(0'u64, streamPayload, 0'u64, false)
      let targetRemote = initTAddress("10.0.0.88", Port(4888))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)

      var streamHandle: HQUIC
      check api.StreamOpen(connection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr streamHandle) ==
        QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(streamHandle)

      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = streamFrame,
        targetRemote = targetRemote
      )
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        4'u64,
        base - 10_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = proto.encodeStreamFrame(0'u64, @[0x44'u8], 0'u64, false)
      )

      check attachSentPacketStreamHandleForTest(connection, 1'u64, packet_model.ceOneRtt, streamHandle)
      check applyAckForTest(connection, packet_model.ceOneRtt, 4'u64)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check frameKind == sfkStream
      check payloadLen == uint32(streamFrame.len)
      check payload == streamFrame
      check port == 4888'u16
      check host.contains("10.0.0.88")
    )

  test "loss timer retransmits lost one-rtt stream frames immediately":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let streamPayload = @[0x55'u8, 0x66, 0x77]
      let streamFrame = proto.encodeStreamFrame(0'u64, streamPayload, 0'u64, false)
      let targetRemote = initTAddress("10.0.0.99", Port(4999))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)

      var streamHandle: HQUIC
      check api.StreamOpen(connection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr streamHandle) ==
        QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(streamHandle)

      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = streamFrame,
        targetRemote = targetRemote
      )
      check attachSentPacketStreamHandleForTest(connection, 1'u64, packet_model.ceOneRtt, streamHandle)
      check setConnectionLossTimeForTest(connection, packet_model.ceOneRtt, base - 10_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check frameKind == sfkStream
      check payloadLen == uint32(streamFrame.len)
      check payload == streamFrame
      check port == 4999'u16
      check host.contains("10.0.0.99")
    )

  test "ack-driven loss retransmits lost one-rtt reset-stream control frames":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let resetPayload = proto.encodeResetStreamFrame(0'u64, 7'u64, 0'u64)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkResetStream,
        framePayload = resetPayload
      )
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        4'u64,
        base - 10_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = proto.encodeStreamFrame(0'u64, @[0x44'u8], 0'u64, false)
      )

      check applyAckForTest(connection, packet_model.ceOneRtt, 4'u64)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check frameKind == sfkResetStream
      check payloadLen == uint32(resetPayload.len)
      check payload == resetPayload
    )

  test "loss timer retransmits lost one-rtt stop-sending control frames":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let stopPayload = proto.encodeStopSendingFrame(0'u64, 9'u64)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkStopSending,
        framePayload = stopPayload
      )
      check setConnectionLossTimeForTest(connection, packet_model.ceOneRtt, base - 10_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check count == 1'u32
      check frameKind == sfkStopSending
      check payloadLen == uint32(stopPayload.len)
      check payload == stopPayload
    )

  test "ack-driven loss retransmits lost one-rtt path-challenge to original remote":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let challengeData = [byte 9, 8, 7, 6, 5, 4, 3, 2]
      let challengePayload = proto.encodePathChallengeFrame(challengeData)
      let targetRemote = initTAddress("10.0.0.44", Port(4444))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkPathChallenge,
        framePayload = challengePayload,
        targetRemote = targetRemote
      )
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        4'u64,
        base - 10_000'u64,
        1200'u16,
        frameKind = sfkStream,
        framePayload = proto.encodeStreamFrame(0'u64, @[0x44'u8], 0'u64, false)
      )

      check applyAckForTest(connection, packet_model.ceOneRtt, 4'u64)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check count == 1'u32
      check frameKind == sfkPathChallenge
      check payloadLen == uint32(challengePayload.len)
      check payload == challengePayload
      check port == 4444'u16
      check host.contains("10.0.0.44")
    )

  test "loss timer retransmits lost one-rtt path-response to original remote":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let base = nowUs()
      let responseData = [byte 4, 3, 2, 1, 0, 1, 2, 3]
      let responsePayload = proto.encodePathResponseFrame(responseData)
      let targetRemote = initTAddress("10.0.0.55", Port(4555))
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check seedSentPacketForTest(
        connection,
        packet_model.ceOneRtt,
        1'u64,
        base - 500_000'u64,
        1200'u16,
        frameKind = sfkPathResponse,
        framePayload = responsePayload,
        targetRemote = targetRemote
      )
      check setConnectionLossTimeForTest(connection, packet_model.ceOneRtt, base - 10_000'u64)
      check recomputeConnectionLossDetectionTimerForTest(connection)

      check runConnectionLossRecoveryTick(connection)

      var payloadLen = 0'u32
      var payload: seq[byte] = @[]
      var frameKind = sfkAck
      var count = 0'u32
      var host = ""
      var port = 0'u16
      check getConnectionLastSentFramePayloadLenForTest(connection, payloadLen)
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check count == 1'u32
      check frameKind == sfkPathResponse
      check payloadLen == uint32(responsePayload.len)
      check payload == responsePayload
      check port == 4555'u16
      check host.contains("10.0.0.55")
    )

  test "pending control-frame queue deduplicates non-head duplicates":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      let frameA = proto.encodeHandshakeDoneFrame()
      let frameB = proto.encodePathResponseFrame([byte 1, 2, 3, 4, 5, 6, 7, 8])

      check prependPendingControlFrameForTest(connection, packet_model.ceOneRtt, sfkHandshakeDone, frameA)
      check prependPendingControlFrameForTest(connection, packet_model.ceOneRtt, sfkPathResponse, frameB)
      check prependPendingControlFrameForTest(connection, packet_model.ceOneRtt, sfkHandshakeDone, frameA)

      var count = 0'u32
      check getConnectionPendingControlFrameCountForTest(connection, count)
      check count == 2'u32
    )

  test "pending stream queue deduplicates non-head duplicates":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard registration
      discard api

      var streamHandle: HQUIC
      check api.StreamOpen(connection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr streamHandle) ==
        QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(streamHandle)

      check prependPendingStreamChunkForTest(streamHandle, 0'u64, @[0x10'u8, 0x20], false)
      check prependPendingStreamChunkForTest(streamHandle, 2'u64, @[0x30'u8], true)
      check prependPendingStreamChunkForTest(streamHandle, 0'u64, @[0x10'u8, 0x20], false)

      var count = 0'u32
      check getStreamPendingChunkCountForTest(streamHandle, count)
      check count == 2'u32
    )

  test "pending stream queue keeps same chunk on different remotes as distinct":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard registration
      discard api

      var streamHandle: HQUIC
      check api.StreamOpen(connection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr streamHandle) ==
        QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(streamHandle)

      let remoteA = initTAddress("10.0.2.201", Port(5201))
      let remoteB = initTAddress("10.0.2.202", Port(5202))
      check prependPendingStreamChunkForTest(streamHandle, 0'u64, @[0x10'u8, 0x20], false, remoteA)
      check prependPendingStreamChunkForTest(streamHandle, 0'u64, @[0x10'u8, 0x20], false, remoteB)

      var count = 0'u32
      check getStreamPendingChunkCountForTest(streamHandle, count)
      check count == 2'u32
    )

  test "flushStream preserves pending stream chunk original remote":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard registration
      discard api
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check setConnectionHandshakeStateForTest(connection, true)

      var streamHandle: HQUIC
      check api.StreamOpen(connection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr streamHandle) ==
        QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(streamHandle)

      let originalRemote = initTAddress("10.0.2.211", Port(5211))
      check prependPendingStreamChunkForTest(
        streamHandle,
        0'u64,
        @[0x21'u8, 0x31, 0x41],
        false,
        originalRemote
      )
      check setConnectionRemoteAddressForTest(connection, "10.0.2.212", 5212'u16)
      check flushStreamForTest(streamHandle)

      var frameKind = sfkAck
      var host = ""
      var port = 0'u16
      var payload: seq[byte] = @[]
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check frameKind == sfkStream
      check payload == proto.encodeStreamFrame(0'u64, @[0x21'u8, 0x31, 0x41], 0'u64, false)
      check port == 5211'u16
      check host.contains("10.0.2.211")
    )

  test "flushPendingDatagrams preserves pending datagram original remote":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard registration
      check MsQuicEnableDatagramSendShim(connection, BOOLEAN(1)) == QUIC_STATUS_SUCCESS
      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check setConnectionRemoteAddressForTest(connection, "10.0.2.221", 5221'u16)

      var datagramBytes = [byte 0x31, 0x41, 0x51]
      var datagramBuf = QuicBuffer(
        Length: 3'u32,
        Buffer: cast[ptr uint8](unsafeAddr datagramBytes[0])
      )
      check api.DatagramSend(
        connection,
        addr datagramBuf,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check setConnectionRemoteAddressForTest(connection, "10.0.2.222", 5222'u16)
      check setConnectionHandshakeStateForTest(connection, true)
      check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)
      check flushPendingDatagramsForTest(connection)

      var frameKind = sfkAck
      var host = ""
      var port = 0'u16
      var payload: seq[byte] = @[]
      check getConnectionLastSentFrameKindForTest(connection, frameKind)
      check getConnectionLastSentTargetRemoteForTest(connection, host, port)
      check getConnectionLastSentFramePayloadForTest(connection, payload)
      check frameKind == sfkDatagram
      check payload == proto.encodeDatagramFrame(@[0x31'u8, 0x41, 0x51], true)
      check port == 5221'u16
      check host.contains("10.0.2.221")
    )

  test "pto timeout still sends probes when BBR congestion window is exhausted":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check setConnectionCongestionAlgorithmForTest(connection, caBbr)

      let base = nowUs()
      check prepareConnectionPacketSendForTest(connection, packet_model.ceInitial)
      check setConnectionRttStatsForTest(connection, 100_000'u64, 100_000'u64, 90_000'u64, 50_000'u64)

      var windowBytes = 0'u64
      var allowance = 0'u64
      check getConnectionCongestionWindow(connection, windowBytes)
      check setConnectionCongestionBytesInFlightForTest(connection, uint32(windowBytes))
      check getConnectionCongestionAllowanceForTest(connection, allowance)
      check allowance == 0'u64

      check seedSentPacketForTest(connection, packet_model.ceInitial, 1'u64, base - 500_000'u64, 1200'u16)
      check runConnectionLossRecoveryTick(connection)

      var count = 0'u32
      var probeCount = 0'u16
      var packets = 0'u32
      var bytes = 0'u64
      check getConnectionSentPacketCountForTest(connection, count)
      check getConnectionProbeCountForEpoch(connection, packet_model.ceInitial, probeCount)
      check getConnectionLossFlightForTest(connection, packet_model.ceInitial, packets, bytes)
      check count == 3'u32
      check probeCount == 1'u16
      check packets == 3'u32
      check bytes > 1200'u64
    )

  test "actual epoch send paths consume congestion allowance":
    withConnection(proc(api: ptr QuicApiTable; registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      var before = 0'u64
      var after = 0'u64

      check prepareConnectionPacketSendForTest(connection, packet_model.ceInitial)
      check getConnectionCongestionAllowanceForTest(connection, before)
      check sendConnectionEpochPacketForTest(connection, packet_model.ceInitial, 1200'u32, true)
      check getConnectionCongestionAllowanceForTest(connection, after)
      check after < before

      check prepareConnectionPacketSendForTest(connection, packet_model.ceHandshake)
      check getConnectionCongestionAllowanceForTest(connection, before)
      check sendConnectionEpochPacketForTest(connection, packet_model.ceHandshake, 1200'u32, true)
      check getConnectionCongestionAllowanceForTest(connection, after)
      check after < before

      check prepareConnectionPacketSendForTest(connection, packet_model.ceZeroRtt)
      check getConnectionCongestionAllowanceForTest(connection, before)
      check sendConnectionEpochPacketForTest(connection, packet_model.ceZeroRtt, 1200'u32, true)
      check getConnectionCongestionAllowanceForTest(connection, after)
      check after < before
    )
