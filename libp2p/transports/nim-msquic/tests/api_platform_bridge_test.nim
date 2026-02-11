import std/[unittest, strutils]

import ../api/platform_bridge
import ../api/common
from ../api/api_impl import MsQuicOpenVersion, MsQuicClose, QuicApiTable,
    QUIC_STATUS_SUCCESS, getGlobalExecutionConfigState, HQUIC
import ../platform/common
import ../platform/datapath_model

suite "MsQuic platform bridge (F4)":

  test "derive execution plan respects affinity layout":
    let stats = WorkerPoolStats(
      workerCount: 4,
      usesCoreMasks: true,
      affinityPolicy: "affinitize-high")
    let features = {DatapathFeature.dfRecvSideScaling, DatapathFeature.dfRawSocket}
    let plan = deriveExecutionPlan(
      stats,
      features,
      @[ @[2, 0], @[3] ])
    check efAffinitize in plan.flags
    check efNoIdealProcessor in plan.flags
    check efHighPriority in plan.flags
    check plan.processors == @[uint16(2), uint16(0), uint16(3)]
    check plan.pollingIdleTimeoutUs > 0

  test "apply execution plan writes MsQuic global config":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer: MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)
    let bridge = initPlatformBridge(api)
    var plan = deriveExecutionPlan(
      WorkerPoolStats(workerCount: 4, usesCoreMasks: true, affinityPolicy: "manual"),
      {DatapathFeature.dfRecvSideScaling},
      @[ @[1, 3] ])
    check bridge.applyExecutionPlan(plan) == QUIC_STATUS_SUCCESS
    let state = getGlobalExecutionConfigState()
    check state.applied
    let expectedMask = 0x0008'u32 or 0x0010'u32 or 0x0020'u32
    check state.flags == expectedMask
    check state.processors == plan.processors
    check state.pollingIdleTimeoutUs == plan.pollingIdleTimeoutUs

  test "open registration chooses throughput profile for RSS datapath":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)
    let bridge = initPlatformBridge(api)

    var stats = WorkerPoolStats(workerCount: 6, usesCoreMasks: false, affinityPolicy: "max-throughput")
    let datapath = DatapathCommon(
      udpHandlersPresent: true,
      tcpHandlersPresent: false,
      workerStats: stats,
      features: {DatapathFeature.dfRecvSideScaling},
      hasRawPath: false)

    let config = QuicRegistrationConfig(
      appName: "nim-bridge",
      profile: qepLowLatency,
      autoCloseConnections: false,
      enableExecutionPolling: false)

    var registration: HQUIC
    var applied: QuicExecutionProfile
    check bridge.openRegistrationWithPlatform(
      config,
      stats,
      datapath,
      addr registration,
      applied) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)
      MsQuicClose(apiPtr)

    check applied == qepMaxThroughput
    check registration != nil

  test "binding plan covers raw share semantics":
    let socket = SocketCommon(
      localAddress: "",
      remoteAddress: "",
      datapathType: dtRaw,
      socketType: SocketType.stUdp,
      flags: {SocketFlags.sfShare, SocketFlags.sfXdp},
      mtu: 1500'u16,
      clientContextName: "dp")
    let plan = deriveBindingPlan(socket, 4433)
    check plan.address == "0.0.0.0"
    check plan.port == 4433'u16
    check plan.rawMode
    check plan.shareBinding
    let summary = bindingSummary(plan)
    check summary.contains("raw")
