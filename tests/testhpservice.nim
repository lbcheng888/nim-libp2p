{.used.}

# Nim-LibP2P
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.used.}

{.push raises: [].}

import chronos

import unittest2
import ./helpers
import ./stubs/switchstub
import ../libp2p/[builders, switch, wire, connmanager, services/hpservice, services/autorelayservice]
import ../libp2p/protocols/ping
import ../libp2p/protocols/connectivity/relay/[relay, client]
import ../libp2p/protocols/connectivity/autonat/[service]
import ../libp2p/nameresolving/[nameresolver, mockresolver]
import stubs/autonatclientstub

proc createSwitch(
    r: Relay = nil, hpService: Service = nil, nameResolver: NameResolver = nil
): Switch {.raises: [LPError].} =
  var builder = SwitchBuilder
    .new()
    .withRng(newRng())
    .withAddresses(@[MultiAddress.init("/ip4/0.0.0.0/tcp/0").tryGet()])
    .withTcpTransport()
    .withMplex()
    .withAutonat()
    .withNoise()

  if hpService != nil:
    builder = builder.withServices(@[hpService])

  if r != nil:
    builder = builder.withCircuitRelay(r)

  if nameResolver != nil:
    builder = builder.withNameResolver(nameResolver)

  return builder.build()

proc buildRelayMA(switchRelay: Switch, switchClient: Switch): MultiAddress =
  MultiAddress
  .init(
    $switchRelay.peerInfo.addrs[0] & "/p2p/" & $switchRelay.peerInfo.peerId &
      "/p2p-circuit/p2p/" & $switchClient.peerInfo.peerId
  )
  .get()

suite "HPService guards":
  test "new stores explicit config":
    let cfg = HPServiceConfig.init(
      directProbeTimeout = 3.seconds,
      maxProbeCandidates = 4,
      keepaliveInterval = 9.seconds,
      relayCloseDelay = 1.seconds,
      upgradeDelay = 250.milliseconds,
      dcutrConnectTimeout = 11.seconds,
      maxDcutrDialableAddrs = 5,
    )
    let autonatClientStub = AutonatClientStub.new()
    let autonatService =
      AutonatService.new(autonatClientStub, newRng(), maxQueueSize = 1)
    proc noop(addresses: seq[MultiAddress]) = discard
    let autoRelayService = AutoRelayService.new(0, RelayClient.new(), noop, newRng())
    let hpservice = HPService.new(autonatService, autoRelayService, config = cfg)
    check hpservice.config.directProbeTimeout == 3.seconds
    check hpservice.config.maxProbeCandidates == 4
    check hpservice.config.keepaliveInterval == 9.seconds
    check hpservice.config.relayCloseDelay == 1.seconds
    check hpservice.config.upgradeDelay == 250.milliseconds
    check hpservice.config.dcutrConnectTimeout == 11.seconds
    check hpservice.config.maxDcutrDialableAddrs == 5

  asyncTest "newConnectedPeerHandler tolerates missing peer entry":
    let autonatClientStub = AutonatClientStub.new()
    let autonatService =
      AutonatService.new(autonatClientStub, newRng(), maxQueueSize = 1)
    proc noop(addresses: seq[MultiAddress]) = discard
    let autoRelayService = AutoRelayService.new(0, RelayClient.new(), noop, newRng())
    let hpservice = HPService.new(autonatService, autoRelayService)
    let sw = createSwitch()
    let event = PeerEvent(kind: PeerEventKind.Joined, initiator: false)
    check await hpservice.setup(sw)
    await sw.connManager.triggerPeerEvents(sw.peerInfo.peerId, event)
    discard await hpservice.stop(sw)

  asyncTest "connectionEventHandler tolerates missing peer entry":
    let autonatClientStub = AutonatClientStub.new()
    let autonatService =
      AutonatService.new(autonatClientStub, newRng(), maxQueueSize = 1)
    proc noop(addresses: seq[MultiAddress]) = discard
    let autoRelayService = AutoRelayService.new(0, RelayClient.new(), noop, newRng())
    let hpservice = HPService.new(autonatService, autoRelayService)
    let sw = createSwitch()
    check await hpservice.setup(sw)
    await sw.connManager.triggerConnEvent(
      sw.peerInfo.peerId, ConnEvent(kind: ConnEventKind.Disconnected)
    )
    discard await hpservice.stop(sw)

  asyncTest "setup mounts ping protocol for direct path keepalive":
    let autonatClientStub = AutonatClientStub.new()
    let autonatService =
      AutonatService.new(autonatClientStub, newRng(), maxQueueSize = 1)
    proc noop(addresses: seq[MultiAddress]) = discard
    let autoRelayService = AutoRelayService.new(0, RelayClient.new(), noop, newRng())
    let hpservice = HPService.new(autonatService, autoRelayService)
    let sw = createSwitch()
    check await hpservice.setup(sw)
    check PingCodec in sw.peerInfo.protocols
    discard await hpservice.stop(sw)

when defined(libp2p_run_hp_tests):
  proc registerHolePunchingSuite() =
    suite "Hole Punching":
      teardown:
        checkTrackers()

      asyncTest "Direct connection must work when peer address is public":
        let autonatClientStub = AutonatClientStub.new(expectedDials = 1)
        autonatClientStub.answer = NotReachable
        let autonatService =
          AutonatService.new(autonatClientStub, newRng(), maxQueueSize = 1)

        let relayClient = RelayClient.new()
        let privatePeerRelayAddr = newFuture[seq[MultiAddress]]()

        let publicPeerSwitch = createSwitch(RelayClient.new())

        proc checkMA(address: seq[MultiAddress]) =
          if not privatePeerRelayAddr.completed():
            privatePeerRelayAddr.complete(address)

        let autoRelayService = AutoRelayService.new(1, relayClient, checkMA, newRng())

        let hpservice = HPService.new(autonatService, autoRelayService)

        let privatePeerSwitch =
          createSwitch(relayClient, hpservice, nameresolver = MockResolver.default())
        let peerSwitch = createSwitch()
        let switchRelay = createSwitch(Relay.new())

        await allFuturesThrowing(
          switchRelay.start(),
          privatePeerSwitch.start(),
          publicPeerSwitch.start(),
          peerSwitch.start(),
        )
        publicPeerSwitch.peerInfo.addrs.add(
          [
            MultiAddress.init("/dns4/localhost/").tryGet() &
              publicPeerSwitch.peerInfo.addrs[0][1].tryGet()
          ]
        )

        await privatePeerSwitch.connect(
          switchRelay.peerInfo.peerId, switchRelay.peerInfo.addrs
        )
        await privatePeerSwitch.connect(
          peerSwitch.peerInfo.peerId, peerSwitch.peerInfo.addrs
        ) # for autonat

        await publicPeerSwitch.connect(
          privatePeerSwitch.peerInfo.peerId, (await privatePeerRelayAddr)
        )

        checkUntilTimeoutCustom(10.seconds, 100.milliseconds):
          privatePeerSwitch.connManager.connCount(publicPeerSwitch.peerInfo.peerId) == 1
          not isRelayed(
            privatePeerSwitch.connManager.selectMuxer(publicPeerSwitch.peerInfo.peerId).connection
          )

        await allFuturesThrowing(
          privatePeerSwitch.stop(),
          publicPeerSwitch.stop(),
          switchRelay.stop(),
          peerSwitch.stop(),
        )

      asyncTest "Direct connection must work when peer address is public and dns is used":
        let autonatClientStub = AutonatClientStub.new(expectedDials = 1)
        autonatClientStub.answer = NotReachable
        let autonatService =
          AutonatService.new(autonatClientStub, newRng(), maxQueueSize = 1)

        let relayClient = RelayClient.new()
        let privatePeerRelayAddr = newFuture[seq[MultiAddress]]()

        let publicPeerSwitch = createSwitch(RelayClient.new())

        proc checkMA(address: seq[MultiAddress]) =
          if not privatePeerRelayAddr.completed():
            privatePeerRelayAddr.complete(address)

        let autoRelayService = AutoRelayService.new(1, relayClient, checkMA, newRng())

        let hpservice = HPService.new(autonatService, autoRelayService)

        let privatePeerSwitch =
          createSwitch(relayClient, hpservice, nameResolver = MockResolver.default())
        let peerSwitch = createSwitch()
        let switchRelay = createSwitch(Relay.new())

        await allFuturesThrowing(
          switchRelay.start(),
          privatePeerSwitch.start(),
          publicPeerSwitch.start(),
          peerSwitch.start(),
        )
        publicPeerSwitch.peerInfo.addrs.add(
          [
            MultiAddress.init("/dns4/localhost/").tryGet() &
              publicPeerSwitch.peerInfo.addrs[0][1].tryGet()
          ]
        )

        await privatePeerSwitch.connect(
          switchRelay.peerInfo.peerId, switchRelay.peerInfo.addrs
        )
        await privatePeerSwitch.connect(
          peerSwitch.peerInfo.peerId, peerSwitch.peerInfo.addrs
        ) # for autonat

        await publicPeerSwitch.connect(
          privatePeerSwitch.peerInfo.peerId, (await privatePeerRelayAddr)
        )

        checkUntilTimeoutCustom(10.seconds, 100.milliseconds):
          privatePeerSwitch.connManager.connCount(publicPeerSwitch.peerInfo.peerId) == 1
          not isRelayed(
            privatePeerSwitch.connManager.selectMuxer(publicPeerSwitch.peerInfo.peerId).connection
          )

        await allFuturesThrowing(
          privatePeerSwitch.stop(),
          publicPeerSwitch.stop(),
          switchRelay.stop(),
          peerSwitch.stop(),
        )

      proc holePunchingTest(
          initiatorConnectStub: connectStubType,
          rcvConnectStub: connectStubType,
          answer: Answer,
      ) {.async.} =
        # There's no check in this test cause it can't test hole punching locally. It exists just to be sure the rest of
        # the code works properly.

        let autonatClientStub1 = AutonatClientStub.new(expectedDials = 1)
        autonatClientStub1.answer = NotReachable
        let autonatService1 =
          AutonatService.new(autonatClientStub1, newRng(), maxQueueSize = 1)

        let autonatClientStub2 = AutonatClientStub.new(expectedDials = 1)
        autonatClientStub2.answer = answer
        let autonatService2 =
          AutonatService.new(autonatClientStub2, newRng(), maxQueueSize = 1)

        let relayClient1 = RelayClient.new()
        let relayClient2 = RelayClient.new()
        let privatePeerRelayAddr1 = newFuture[seq[MultiAddress]]()

        proc checkMA(address: seq[MultiAddress]) =
          if not privatePeerRelayAddr1.completed():
            privatePeerRelayAddr1.complete(address)

        let autoRelayService1 = AutoRelayService.new(1, relayClient1, checkMA, newRng())
        let autoRelayService2 = AutoRelayService.new(1, relayClient2, nil, newRng())

        let hpservice1 = HPService.new(autonatService1, autoRelayService1)
        let hpservice2 = HPService.new(autonatService2, autoRelayService2)

        let privatePeerSwitch1 = SwitchStub.new(
          createSwitch(relayClient1, hpservice1, nameresolver = MockResolver.default())
        )
        let privatePeerSwitch2 = SwitchStub.new(createSwitch(relayClient2, hpservice2))

        let switchRelay = createSwitch(Relay.new())
        let switchAux = createSwitch()
        let switchAux2 = createSwitch()
        let switchAux3 = createSwitch()
        let switchAux4 = createSwitch()

        var awaiter = newFuture[void]()

        await allFuturesThrowing(
          switchRelay.start(),
          privatePeerSwitch1.start(),
          privatePeerSwitch2.start(),
          switchAux.start(),
          switchAux2.start(),
          switchAux3.start(),
          switchAux4.start(),
        )

        await privatePeerSwitch1.connect(
          switchRelay.peerInfo.peerId, switchRelay.peerInfo.addrs
        )
        await privatePeerSwitch2.connect(
          switchAux.peerInfo.peerId, switchAux.peerInfo.addrs
        )

        await sleepAsync(100.millis)

        await privatePeerSwitch1.connect(
          switchAux2.peerInfo.peerId, switchAux2.peerInfo.addrs
        )
        await privatePeerSwitch1.connect(
          switchAux3.peerInfo.peerId, switchAux3.peerInfo.addrs
        )
        await privatePeerSwitch1.connect(
          switchAux4.peerInfo.peerId, switchAux4.peerInfo.addrs
        )

        await privatePeerSwitch2.connect(
          switchAux2.peerInfo.peerId, switchAux2.peerInfo.addrs
        )
        await privatePeerSwitch2.connect(
          switchAux3.peerInfo.peerId, switchAux3.peerInfo.addrs
        )
        await privatePeerSwitch2.connect(
          switchAux4.peerInfo.peerId, switchAux4.peerInfo.addrs
        )

        privatePeerSwitch1.connectStub = initiatorConnectStub
        await privatePeerSwitch2.connect(
          privatePeerSwitch1.peerInfo.peerId, (await privatePeerRelayAddr1)
        )
        privatePeerSwitch2.connectStub = rcvConnectStub

        # wait for hole punching to finish in the background
        await sleepAsync(300.millis)

        await allFuturesThrowing(
          privatePeerSwitch1.stop(),
          privatePeerSwitch2.stop(),
          switchRelay.stop(),
          switchAux.stop(),
          switchAux2.stop(),
          switchAux3.stop(),
          switchAux4.stop(),
        )

      asyncTest "Hole punching when peers addresses are private":
        await holePunchingTest(nil, nil, NotReachable)

      asyncTest "Hole punching when peers addresses are private and there is an error in the initiator side":
        proc connectStub(
            self: SwitchStub,
            peerId: PeerId,
            addrs: seq[MultiAddress],
            forceDial = false,
            reuseConnection = true,
            dir = Direction.Out,
        ): Future[void] {.async: (raises: [DialFailedError, CancelledError]).} =
          self.connectStub = nil # this stub should be called only once
          raise newException(DialFailedError, "error")

        await holePunchingTest(connectStub, nil, Reachable)

else:
  proc registerHolePunchingSuite() =
    suite "Hole Punching":
      test "Hole Punching tests disabled":
        skip()

registerHolePunchingSuite()
