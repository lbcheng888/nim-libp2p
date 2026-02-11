when defined(linux) and defined(amd64):
  when defined(libp2p_run_mdns_tests):
    {.used.}

    import chronos
    import stew/byteutils
    import unittest2

    import ../../helpers
    import ../../../libp2p/discovery/[mdns, discoverymngr]
    import ../../../libp2p/[peerid, multiaddress, multicodec, builders]
    import ../../../libp2p/protocols/pubsub/gossipsub
    import ../../../libp2p/protocols/dm/dmservice
    import ../../../libp2p/protocols/secure/secure
    import ../../../libp2p/protocols/pubsub/rpc/message

    const MdnsTestService = "_nimlibp2p_test._udp.local"

    suite "Connectivity - mDNS Discovery":
      teardown:
        checkTrackers()

      proc newGossipNode(addrs: seq[string]): GossipSub =
        let ma = addrs.mapIt(MultiAddress.init(it).tryGet())
        let switch = newStandardSwitch(
          addrs = ma,
          secureManagers = [SecureProtocol.Noise],
          sendSignedPeerRecord = false,
        )
        let g = GossipSub.init(
          switch = switch,
          triggerSelf = false,
          verifySignature = false,
          sign = false,
          msgIdProvider = defaultMsgIdProvider,
          anonymize = false,
        )
        switch.mount(g)
        g

      asyncTest "mdns discovery yields IPv4 and IPv6 addresses and dm succeeds":
        let listener = newGossipNode(
          [
            "/ip4/127.0.0.1/tcp/0",
            "/ip6/::1/tcp/0",
          ]
        )
        let dialer = newGossipNode(["/ip4/127.0.0.1/tcp/0"])

        await listener.switch.start()
        await dialer.switch.start()
        defer:
          await listener.switch.stop()
          await dialer.switch.stop()

        let advertiserMdns = MdnsInterface.new(
          listener.switch.peerInfo,
          serviceName = MdnsTestService,
          announceInterval = 500.milliseconds,
          queryInterval = 500.milliseconds,
        )
        advertiserMdns.setPreferredIpv4("127.0.0.1")
        let explorerMdns = MdnsInterface.new(
          dialer.switch.peerInfo,
          serviceName = MdnsTestService,
          announceInterval = 500.milliseconds,
          queryInterval = 500.milliseconds,
        )
        explorerMdns.setPreferredIpv4("127.0.0.1")

        let advertiserDM = DiscoveryManager()
        advertiserDM.add(advertiserMdns)
        advertiserDM.advertise(DiscoveryService(MdnsTestService))

        let explorerDM = DiscoveryManager()
        explorerDM.add(explorerMdns)
        explorerDM.advertise(DiscoveryService(MdnsTestService))

        let query = explorerDM.request(DiscoveryService(MdnsTestService))
        let peerFuture = query.getPeer()
        doAssert await peerFuture.withTimeout(5.seconds), "mDNS discovery timed out"
        let attrs = await peerFuture

        query.stop()
        explorerDM.stop()
        advertiserDM.stop()
        await advertiserMdns.closeTransport()
        await explorerMdns.closeTransport()

        let discoveredPeer = attrs[PeerId]
        let addrs = attrs.getAll(MultiAddress)
        check addrs.len > 0
        check addrs.anyIt(it.hasProto(multiCodec("ip4")))
        check addrs.anyIt(it.hasProto(multiCodec("ip6")))

        await dialer.switch.connect(discoveredPeer, addrs)

        checkUntilTimeout:
          dialer.switch.connManager.connCount(listener.switch.peerInfo.peerId) == 1
          listener.switch.connManager.connCount(dialer.switch.peerInfo.peerId) == 1

        let dmReceived = newFuture[seq[byte]]("mdns-dm-received")

        let dmListener = newDirectMessageService(
          listener,
          listener.peerInfo.peerId,
          proc(msg: DirectMessage) {.async.} =
            if not dmReceived.finished:
              dmReceived.complete(msg.payload)
        )

        let dmDialer = newDirectMessageService(
          dialer,
          dialer.peerInfo.peerId,
          proc(msg: DirectMessage) {.async.} =
            discard
        )

        await waitSub(dialer, listener, dmListener.localDmTopic())
        await waitSub(listener, dialer, dmDialer.localDmTopic())
        await waitSub(listener, dialer, dmDialer.localAckTopic())

        let payload = "mdns-dm payload".toBytes()
        let delivered = await dmDialer.send(listener.peerInfo.peerId, payload)
        check delivered
        check (await dmReceived) == payload
  else:
    import unittest2

    suite "Connectivity - mDNS Discovery":
      test "mdns connectivity tests disabled":
        skip()
else:
  import unittest2

  suite "Connectivity - mDNS Discovery":
    test "mdns connectivity tests unsupported platform":
      skip()
