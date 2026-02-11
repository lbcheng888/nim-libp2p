when defined(linux) and defined(amd64):
  {.used.}

  import chronos
  import std/times
  import stew/byteutils
  import unittest2

  import ../../helpers
  import ../../pubsub/utils
  import ../../../libp2p/protocols/pubsub/gossipsub
  import ../../../libp2p/protocols/dm/dmservice

  suite "Connectivity - Direct TCP":
    teardown:
      checkTrackers()

    asyncTest "static address connection delivers direct message":
      let nodes = generateNodes(
        2,
        gossip = true,
        triggerSelf = false,
        verifySignature = false,
        sign = false,
        floodPublish = true,
      )
      await startNodes(nodes)
      defer:
        await stopNodes(nodes)

      let gossip = nodes.toGossipSub()
      let dialer = gossip[1]
      let listener = gossip[0]

      await dialer.switch.connect(listener.switch.peerInfo.peerId, listener.switch.peerInfo.addrs)

      checkUntilTimeout:
        dialer.switch.connManager.connCount(listener.switch.peerInfo.peerId) == 1
        listener.switch.connManager.connCount(dialer.switch.peerInfo.peerId) == 1

      let dmReceived = newFuture[seq[byte]]("dm-direct-connect")

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

      let payload = "nim-libp2p-connectivity-check".toBytes()
      let mid = "dm-" & $getTime().toUnix()
      let delivery = await dmDialer.send(
        listener.peerInfo.peerId,
        payload,
        ackRequested = true,
        messageId = mid,
        timeout = chronos.milliseconds(5000)
      )
      check delivery[0]
      check (await dmReceived) == payload
else:
  import unittest2

  suite "Connectivity - Direct TCP":
    test "connectivity tests disabled on this platform":
      skip()
