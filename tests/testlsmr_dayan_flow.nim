{.used.}

import std/[options, sequtils]

import unittest2

import ../libp2p/[crypto/crypto, lsmr]

proc makePeer(
    rng: ref HmacDrbgContext,
    prefix: LsmrPath,
    subscriptions: seq[LsmrDaYanSubscription],
): LsmrDaYanPeer =
  let key = PrivateKey.random(ECDSA, rng[]).tryGet()
  LsmrDaYanPeer.init(PeerId.init(key).tryGet(), prefix, subscriptions)

suite "LSMR DaYan Flow":
  test "subscription filter enforces mode resolution topology semantic and guard":
    let sub =
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8, 1'u8],
        minResolution = 2'u8,
        maxResolution = 3'u8,
        semanticPrefix = "sem:",
        guardPrefix = "guard:",
        acceptUrgent = false,
        acceptBackground = true,
      )
    check sub.isValid()

    let urgentTopic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:alarm",
        guardDigest = "guard:safe",
        mode = LsmrDaYanMode.ldymZhen,
      )
    let backgroundTopic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:alarm",
        guardDigest = "guard:safe",
        mode = LsmrDaYanMode.ldymXun,
      )

    check sub.evaluate(urgentTopic) == LsmrDaYanFilterResult.ldyfBlockMode
    check sub.evaluate(backgroundTopic) == LsmrDaYanFilterResult.ldyfAccept
    check sub.evaluate(backgroundTopic, resolution = 1'u8) ==
      LsmrDaYanFilterResult.ldyfBlockResolution
    check sub.evaluate(
      LsmrDaYanTopic.init(
        center = @[5'u8, 9'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:alarm",
        guardDigest = "guard:safe",
      )
    ) == LsmrDaYanFilterResult.ldyfBlockTopology
    check sub.evaluate(
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "other:alarm",
        guardDigest = "guard:safe",
      )
    ) == LsmrDaYanFilterResult.ldyfBlockSemantic
    check sub.evaluate(
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:alarm",
        guardDigest = "unsafe",
      )
    ) == LsmrDaYanFilterResult.ldyfBlockGuard

  test "dayan flow plans kun dui li stages deterministically":
    let rng = newRng()
    let topic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8, 9'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:plant:alarm",
        guardDigest = "guard:safe",
        mode = LsmrDaYanMode.ldymZhen,
      )
    let localSub =
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8, 1'u8, 9'u8],
        minResolution = 3'u8,
        maxResolution = 3'u8,
        semanticPrefix = "sem:",
        acceptUrgent = true,
        acceptBackground = false,
      )
    let regionalSub =
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8, 1'u8],
        minResolution = 2'u8,
        maxResolution = 3'u8,
        semanticPrefix = "sem:",
      )
    let globalSub =
      LsmrDaYanSubscription.init(
        topologyPrefix = @[5'u8],
        minResolution = 1'u8,
        maxResolution = 2'u8,
        semanticPrefix = "sem:",
      )

    let localPeer = makePeer(rng, @[5'u8, 1'u8, 9'u8], @[localSub])
    let regionalPeer = makePeer(rng, @[5'u8, 1'u8, 8'u8], @[regionalSub])
    let globalNearPeer = makePeer(rng, @[5'u8, 5'u8, 4'u8], @[globalSub])
    let globalFarPeer = makePeer(rng, @[5'u8, 9'u8, 2'u8], @[globalSub])
    let wrongModePeer = makePeer(
      rng,
      @[5'u8, 1'u8, 9'u8],
      @[
        LsmrDaYanSubscription.init(
          topologyPrefix = @[5'u8, 1'u8, 9'u8],
          minResolution = 3'u8,
          maxResolution = 3'u8,
          semanticPrefix = "sem:",
          acceptUrgent = false,
          acceptBackground = true,
        )
      ],
    )
    let peers = @[wrongModePeer, globalFarPeer, localPeer, globalNearPeer, regionalPeer]

    let routes =
      planDaYanFlow(
        peers,
        topic,
        LsmrDaYanFlowConfig.init(
          regionalFanout = 1,
          globalFanout = 1,
          minGlobalResolution = 1'u8,
        ),
      )

    check routes.len == 4
    check routes.mapIt(it.stage) == @[
      LsmrDaYanStage.ldysKunLocal,
      LsmrDaYanStage.ldysDuiRegional,
      LsmrDaYanStage.ldysLiGlobal,
      LsmrDaYanStage.ldysLiGlobal,
    ]
    check routes.mapIt(it.payloadKind) == @[
      LsmrDaYanPayloadKind.ldypkCsgDelta,
      LsmrDaYanPayloadKind.ldypkCsgDelta,
      LsmrDaYanPayloadKind.ldypkSemanticFingerprint,
      LsmrDaYanPayloadKind.ldypkSemanticFingerprint,
    ]
    check routes.mapIt(it.resolution) == @[3'u8, 2'u8, 2'u8, 1'u8]
    check routes[0].peerId == localPeer.peerId
    check routes[1].peerId == regionalPeer.peerId
    check routes[2].peerId == globalNearPeer.peerId
    check routes[3].peerId == globalFarPeer.peerId
    check routes[0].topologyPrefix == @[5'u8, 1'u8, 9'u8]
    check routes[1].topologyPrefix == @[5'u8, 1'u8]
    check routes[3].topologyPrefix == @[5'u8]

  test "exclude peer and invalid topic shrink flow to empty":
    let rng = newRng()
    let topic =
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8],
        resolution = 2'u8,
        semanticFingerprint = "sem:sync",
      )
    let peer = makePeer(
      rng,
      @[5'u8, 1'u8],
      @[
        LsmrDaYanSubscription.init(
          topologyPrefix = @[5'u8, 1'u8],
          minResolution = 2'u8,
          maxResolution = 2'u8,
          semanticPrefix = "sem:",
        )
      ],
    )

    let excluded = planDaYanFlow(@[peer], topic, excludePeerId = some(peer.peerId))
    check excluded.len == 0
    check planDaYanFlow(
      @[peer],
      LsmrDaYanTopic.init(
        center = @[5'u8, 1'u8],
        resolution = 3'u8,
        semanticFingerprint = "sem:sync",
      ),
    ).len == 0
