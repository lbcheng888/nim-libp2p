{.used.}

import chronos
import unittest2

import ../libp2p/[builders, switch]
import ../libp2p/services/wanbootstrapservice
import ./utils/async_tests

proc makeMemorySwitch(): Switch =
  newStandardSwitch(transport = TransportType.Memory)

suite "WAN bootstrap authoritative auto join":
  asyncTest "auto authoritative seed source is not classified as direct hint":
    let sw = makeMemorySwitch()
    await sw.start()
    defer:
      await sw.stop()

    let svc = await startWanBootstrapService(
      sw,
      WanBootstrapConfig.init(
        maxBootstrapCandidates = 2,
        recentStableSlots = 0,
        randomPoolCap = 2,
      ),
    )

    let directPeer = makeMemorySwitch().peerInfo.peerId
    let authoritativePeer = makeMemorySwitch().peerInfo.peerId

    check svc.registerBootstrapHint(
      directPeer,
      @[MultiAddress.init("/ip4/10.0.1.1/tcp/4001").tryGet()],
      source = "invite",
    )
    check svc.registerBootstrapHint(
      authoritativePeer,
      @[MultiAddress.init("/ip4/10.0.1.2/tcp/4001").tryGet()],
      source = "auto_authoritative_seed_tick",
    )

    let selected = svc.selectBootstrapCandidates()
    check selected.len == 2
    check selected[0].peerId == directPeer
    check selected[0].selectionReason == "direct_hint"
    check selected[1].peerId == authoritativePeer
    check selected[1].selectionReason == "hint"
