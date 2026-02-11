## 对应 `src/core/connection*.c/h` 的连接状态蓝图。

import std/sequtils
import ./common
import ./packet_model
import ./stream_model

type
  QuicConnectionFlag* = enum
    qcfAllocated
    qcfInitialized
    qcfHandshakeStarted
    qcfHandshakeComplete
    qcfClosedLocally
    qcfClosedRemotely
    qcfClosedSilently
    qcfAppClosed
    qcfShutdownComplete
    qcfHandleClosed
    qcfFreed
    qcfPartitioned
    qcfCloseAsync
    qcfDisable1RttEncryption
    qcfExternalOwner
    qcfRegistered
    qcfGotFirstServerResponse
    qcfRetryUsed
    qcfHandshakeConfirmed
    qcfListenerAccepted
    qcfLocalAddressSet
    qcfRemoteAddressSet
    qcfPeerTransportParamsValid
    qcfUpdateWorker
    qcfShutdownTimedOut
    qcfProcessShutdownComplete
    qcfShareBinding
    qcfResumptionEnabled
    qcfInlineApiExecution
    qcfVersionNegAttempted
    qcfVersionNegCompleted
    qcfLocalInterfaceSet
    qcfFixedBit
    qcfReliableResetNegotiated
    qcfTimestampSendNegotiated
    qcfTimestampRecvNegotiated
    qcfDelayedApplicationError

  QuicConnectionPhase* = enum
    qcpIdle
    qcpHandshake
    qcpEstablished
    qcpClosing
    qcpDraining

  PathState* = object
    pathId*: uint8
    isActive*: bool
    isValidated*: bool
    smoothedRtt*: uint32
    latestRtt*: uint32
    congestionWindow*: uint32
    challengeOutstanding*: bool
    challengeData*: array[8, uint8]
    responsePending*: bool
    resetToken*: array[16, uint8]

  HandshakeState* = object
    retryUsed*: bool
    confirmed*: bool
    peerTransportParamsValid*: bool
    zeroRttAccepted*: bool

  StreamCollectionState* = object
    scheduling*: StreamSchedulingSnapshot
    openStreams*: array[StreamType, uint64]
    maxPeerStreamCount*: array[StreamType, uint64]
    maxLocalStreamCount*: array[StreamType, uint64]

  TimerState* = object
    lossDetectionTimer*: uint64
    keepAliveTimer*: uint64
    drainTimeout*: uint64

  PreferredAddressState* = object
    ipv4Address*: string
    ipv4Port*: uint16
    ipv6Address*: string
    ipv6Port*: uint16
    hasPreferred*: bool
    cid*: ConnectionId
    statelessResetToken*: array[16, uint8]

  MigrationState* = object
    activePathId*: uint8
    preferredPathId*: uint8
    pendingChallenges*: seq[array[8, uint8]]
    validatedPaths*: seq[uint8]
    preferredAddress*: PreferredAddressState
    statelessResetTokens*: seq[array[16, uint8]]

  QuicConnectionModel* = object
    connectionId*: ConnectionId
    peerConnectionId*: ConnectionId
    attemptedVersion*: QuicVersion
    negotiatedVersion*: QuicVersion
    stateFlags*: set[QuicConnectionFlag]
    phase*: QuicConnectionPhase
    handshake*: HandshakeState
    packetSpaces*: array[CryptoEpoch, PacketNumberSpaceState]
    paths*: seq[PathState]
    streams*: StreamCollectionState
    timers*: TimerState
    migration*: MigrationState

proc derivePhase*(flags: set[QuicConnectionFlag]): QuicConnectionPhase =
  ## 根据标志集合推导连接阶段。
  if qcfClosedLocally in flags or qcfClosedRemotely in flags:
    if qcfShutdownComplete in flags:
      qcpDraining
    else:
      qcpClosing
  elif qcfHandshakeConfirmed in flags or qcfHandshakeComplete in flags:
    qcpEstablished
  elif qcfHandshakeStarted in flags:
    qcpHandshake
  else:
    qcpIdle

proc setPhaseFromFlags*(conn: var QuicConnectionModel) =
  conn.phase = derivePhase(conn.stateFlags)

proc activatePath*(conn: var QuicConnectionModel, pathId: uint8) =
  ## 激活或补齐指定路径，模仿 `connection.c` 中的路径管理逻辑。
  for path in conn.paths.mitems:
    if path.pathId == pathId:
      path.isActive = true
      return
  conn.paths.add PathState(pathId: pathId, isActive: true, isValidated: false,
                           smoothedRtt: 0, latestRtt: 0, congestionWindow: 0,
                           challengeOutstanding: false,
                           challengeData: [uint8 0,0,0,0,0,0,0,0],
                           responsePending: false,
                           resetToken: [uint8 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])

proc ensurePath*(conn: var QuicConnectionModel, pathId: uint8): var PathState =
  for path in conn.paths.mitems:
    if path.pathId == pathId:
      return path
  conn.paths.add PathState(
    pathId: pathId,
    isActive: false,
    isValidated: false,
    smoothedRtt: 0,
    latestRtt: 0,
    congestionWindow: 0,
    challengeOutstanding: false,
    challengeData: [uint8 0,0,0,0,0,0,0,0],
    responsePending: false,
    resetToken: [uint8 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
  result = conn.paths[^1]

proc initiatePathChallenge*(conn: var QuicConnectionModel, pathId: uint8, data: array[8, uint8]) =
  var path = conn.ensurePath(pathId)
  path.challengeOutstanding = true
  path.challengeData = data
  path.responsePending = true
  conn.migration.pendingChallenges.add(data)

proc completePathValidation*(conn: var QuicConnectionModel, pathId: uint8, success: bool) =
  var path = conn.ensurePath(pathId)
  if success:
    path.isValidated = true
    path.isActive = true
    path.challengeOutstanding = false
    path.responsePending = false
    if not conn.migration.validatedPaths.contains(pathId):
      conn.migration.validatedPaths.add(pathId)
    conn.migration.activePathId = pathId
  else:
    path.isActive = false
    path.challengeOutstanding = false
    path.responsePending = false

proc registerStatelessReset*(conn: var QuicConnectionModel, token: array[16, uint8]) =
  conn.migration.statelessResetTokens.add(token)

proc configurePreferredAddress*(conn: var QuicConnectionModel, preferred: PreferredAddressState) =
  conn.migration.preferredAddress = preferred
  conn.migration.preferredPathId = preferred.cid.length
  conn.migration.preferredAddress.hasPreferred = true

proc recordStatelessResetToken*(path: var PathState, token: array[16, uint8]) =
  path.resetToken = token

proc updateHandshakeState*(conn: var QuicConnectionModel, retryUsed, confirmed,
    peerParamsValid, zeroRttAccepted: bool) =
  conn.handshake.retryUsed = retryUsed
  conn.handshake.confirmed = confirmed
  conn.handshake.peerTransportParamsValid = peerParamsValid
  conn.handshake.zeroRttAccepted = zeroRttAccepted
  if confirmed:
    conn.stateFlags.incl(qcfHandshakeConfirmed)
    conn.stateFlags.incl(qcfHandshakeComplete)
  conn.setPhaseFromFlags()

proc newConnectionModel*(cid, peerCid: ConnectionId, attempted, negotiated: QuicVersion): QuicConnectionModel =
  ## 构建 Nim 层连接状态骨架。
  result.connectionId = cid
  result.peerConnectionId = peerCid
  result.attemptedVersion = attempted
  result.negotiatedVersion = negotiated
  for epoch in CryptoEpoch:
    result.packetSpaces[epoch] = initPacketNumberSpace(epoch)
  result.paths = @[]
  result.streams = StreamCollectionState(
    scheduling: StreamSchedulingSnapshot(
      totalStreams: default(array[StreamType, uint32]),
      activeSendOrder: @[],
      useRoundRobin: false),
    openStreams: default(array[StreamType, uint64]),
    maxPeerStreamCount: default(array[StreamType, uint64]),
    maxLocalStreamCount: default(array[StreamType, uint64]))
  result.timers = TimerState(lossDetectionTimer: 0, keepAliveTimer: 0, drainTimeout: 0)
  result.stateFlags = {qcfAllocated, qcfInitialized}
  result.migration = MigrationState(
    activePathId: 0,
    preferredPathId: 0,
    pendingChallenges: @[],
    validatedPaths: @[],
    preferredAddress: PreferredAddressState(
      ipv4Address: "",
      ipv4Port: 0,
      ipv6Address: "",
      ipv6Port: 0,
      hasPreferred: false,
      cid: ConnectionId(length: 0, bytes: [uint8 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]),
      statelessResetToken: [uint8 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]),
    statelessResetTokens: @[])
  result.setPhaseFromFlags()
