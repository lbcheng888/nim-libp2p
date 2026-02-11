import std/[json, sequtils]
from std/times import getTime, toUnix
import chronos
import stew/byteutils
import unittest2
import ../shim

import ./helpers
import ./pubsub/utils
import ../libp2p/discovery/[mdns, discoverymngr]
import ../libp2p/[peerid, multiaddress, multicodec, builders]
import ../libp2p/protocols/pubsub/gossipsub
import ../libp2p/protocols/dm/dmservice
import ../libp2p/protocols/secure/secure
import ../libp2p/protocols/pubsub/rpc/message

const MdnsTestService = "_nimlibp2p_test._udp.local"
const MdnsDmOp = "dm"

proc hasCodec(ma: MultiAddress, codec: MultiCodec): bool =
  let res = ma.protocols()
  if res.isErr:
    return false
  codec in res.get()

suite "Connectivity - mDNS Discovery (mobile)":
  teardown:
    checkTrackers()

  proc newGossipNode(addrs: seq[string]): GossipSub =
    let transportType =
      when defined(libp2p_quic_support):
        TransportType.QUIC
      else:
        TransportType.TCP
    let ma = addrs.mapIt(MultiAddress.init(it).tryGet())
    let switch = newStandardSwitch(
      addrs = ma,
      transport = transportType,
      secureManagers = [SecureProtocol.Noise],
      sendSignedPeerRecord = false,
    )
    let g = GossipSub.init(
      switch = switch,
      triggerSelf = false,
      verifySignature = false,
      sign = false,
      parameters = GossipSubParams.init(floodPublish = true),
    )
    switch.mount(g)
    g

  asyncTest "mdns discovery yields IPv4 and IPv6 addresses and dm succeeds":
    let listenerAddrs =
      when defined(libp2p_quic_support):
        @["/ip4/127.0.0.1/udp/0/quic-v1", "/ip6/::1/udp/0/quic-v1"]
      else:
        @["/ip4/127.0.0.1/tcp/0"]
    let dialerAddrs =
      when defined(libp2p_quic_support):
        @["/ip4/127.0.0.1/udp/0/quic-v1"]
      else:
        @["/ip4/127.0.0.1/tcp/0"]
    let listener = newGossipNode(listenerAddrs)
    let dialer = newGossipNode(dialerAddrs)

    await listener.switch.start()
    await dialer.switch.start()
    defer:
      await listener.switch.stop()
      await dialer.switch.stop()

    let advertiserMdns = block:
      var res: MdnsInterface
      try:
        res = MdnsInterface.new(
          listener.switch.peerInfo,
          serviceName = MdnsTestService,
          announceInterval = 500.milliseconds,
          queryInterval = 500.milliseconds,
        )
      except CatchableError as exc:
        doAssert false, "advertiser mdns init failed: " & exc.msg
      except Exception as exc:
        doAssert false, "advertiser mdns unexpected failure: " & exc.msg
      res
    advertiserMdns.setPreferredIpv4("127.0.0.1")
    let explorerMdns = block:
      var res: MdnsInterface
      try:
        res = MdnsInterface.new(
          dialer.switch.peerInfo,
          serviceName = MdnsTestService,
          announceInterval = 500.milliseconds,
          queryInterval = 500.milliseconds,
        )
      except CatchableError as exc:
        doAssert false, "explorer mdns init failed: " & exc.msg
      except Exception as exc:
        doAssert false, "explorer mdns unexpected failure: " & exc.msg
      res
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
    check addrs.anyIt(hasCodec(it, multiCodec("ip4")))
    when defined(libp2p_quic_support):
      check addrs.anyIt(hasCodec(it, multiCodec("ip6")))

    await dialer.switch.connect(discoveredPeer, addrs)

    checkUntilTimeout:
      dialer.switch.connManager.connCount(listener.switch.peerInfo.peerId) == 1
      listener.switch.connManager.connCount(dialer.switch.peerInfo.peerId) == 1

    let dmReceived = newFuture[seq[byte]]("mdns-dm-received")

    let dmListener = newDirectMessageService(
      listener.switch,
      listener.peerInfo.peerId,
      proc(msg: DirectMessage) {.async.} =
        if not dmReceived.finished:
          dmReceived.complete(msg.payload)
    )

    let dmDialer = newDirectMessageService(
      dialer.switch,
      dialer.peerInfo.peerId,
      proc(msg: DirectMessage) {.async.} =
        discard
    )

    await dmListener.start()
    listener.switch.mount(dmListener)
    await dmDialer.start()
    dialer.switch.mount(dmDialer)

    let mid = "dm-" & $getTime().toUnix()
    var envelope = newJObject()
    envelope["op"] = %MdnsDmOp
    envelope["mid"] = %mid
    envelope["from"] = %($dialer.peerInfo.peerId)
    envelope["body"] = %"mdns-dm payload"
    envelope["timestamp_ms"] = %(int64(getTime().toUnix() * 1000))
    envelope["ackRequested"] = %true
    let messageBytes = $envelope

    let result = await dmDialer.send(
      listener.peerInfo.peerId,
      messageBytes.toBytes(),
      ackRequested = true,
      messageId = mid,
      timeout = chronos.milliseconds(5000)
    )
    check result[0]
    let receivedBytes = await dmReceived
    let receivedText = string.fromBytes(receivedBytes)
    let parsed = parseJson(receivedText)
    check parsed["op"].getStr() == MdnsDmOp
    check parsed["body"].getStr() == "mdns-dm payload"

  asyncTest "mdns discovery tolerates .local suffix mismatch":
    let servicePlain = "_nimlibp2p_test._udp."
    let serviceLocal = "_nimlibp2p_test._udp.local"
    let listener = newGossipNode(@["/ip4/127.0.0.1/tcp/0"])
    let dialer = newGossipNode(@["/ip4/127.0.0.1/tcp/0"])

    await listener.switch.start()
    await dialer.switch.start()
    defer:
      await listener.switch.stop()
      await dialer.switch.stop()

    let advertiserMdns = block:
      var res: MdnsInterface
      try:
        res = MdnsInterface.new(
          listener.switch.peerInfo,
          serviceName = servicePlain,
          announceInterval = 1.seconds,
          queryInterval = 1.seconds
        )
      except CatchableError as exc:
        doAssert false, "advertiser mdns init failed: " & exc.msg
      except Exception as exc:
        doAssert false, "advertiser mdns unexpected failure: " & exc.msg
      res
    advertiserMdns.setPreferredIpv4("127.0.0.1")

    let explorerMdns = block:
      var res: MdnsInterface
      try:
        res = MdnsInterface.new(
          dialer.switch.peerInfo,
          serviceName = serviceLocal,
          announceInterval = 1.seconds,
          queryInterval = 1.seconds
        )
      except CatchableError as exc:
        doAssert false, "explorer mdns init failed: " & exc.msg
      except Exception as exc:
        doAssert false, "explorer mdns unexpected failure: " & exc.msg
      res
    explorerMdns.setPreferredIpv4("127.0.0.1")

    let advertiserDM = DiscoveryManager()
    advertiserDM.add(advertiserMdns)
    advertiserDM.advertise(DiscoveryService(servicePlain))

    let explorerDM = DiscoveryManager()
    explorerDM.add(explorerMdns)

    let query = explorerDM.request(DiscoveryService(serviceLocal))
    defer:
      query.stop()
      explorerDM.stop()
      advertiserDM.stop()
      await advertiserMdns.closeTransport()
      await explorerMdns.closeTransport()

    let fut = query.getPeer()
    doAssert await fut.withTimeout(5.seconds), "mDNS cross-service discovery timed out"
    let attrs = await fut
    let discoveredPeer = attrs[PeerId]
    check discoveredPeer == listener.switch.peerInfo.peerId
    check attrs.getAll(MultiAddress).len > 0
