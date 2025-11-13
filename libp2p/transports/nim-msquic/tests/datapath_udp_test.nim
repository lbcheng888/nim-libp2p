import std/[net, unittest]

import "../platform/mod"

when isMainModule:
  suite "Nim MsQuic Datapath Skeleton":
    test "event loop relays UDP datagrams locally":
      let loop = newEventLoop()
      defer: loop.shutdown()

      var completed = false
      var payloadMatches = false
      var receivedPort = Port(0)
      var addressRecorded = false

      let payload = [0xDE'u8, 0xAD'u8, 0xBE'u8, 0xEF'u8]

      let server = loop.bindUdp("127.0.0.1", Port(0),
        UdpEventHandlers(
          onDatagram: proc(endpoint: UdpEndpoint, msg: UdpMessage) {.closure.} =
            payloadMatches = msg.payload.len == payload.len
            if payloadMatches:
              for idx in 0..<payload.len:
                if msg.payload[idx] != payload[idx]:
                  payloadMatches = false
                  break
            receivedPort = msg.remotePort
            addressRecorded = msg.remoteAddress.len > 0
            completed = true
        ))
      defer: server.close()
      check server.localPort != Port(0)

      let client = loop.bindUdp("127.0.0.1", Port(0))
      defer: client.close()

      let sendFuture = client.sendDatagram(
        initSendRequest("127.0.0.1", server.localPort, payload))

      check loop.runUntil(sendFuture, 200)

      var spinCount = 0
      while not completed and spinCount < 40:
        discard loop.poll(10)
        inc spinCount
      check completed
      check payloadMatches
      check receivedPort == client.localPort
      check addressRecorded
