import std/[asyncdispatch, net, unittest]

import "../platform/mod"

when isMainModule:
  suite "Nim MsQuic 高性能 datapath":
    test "Toeplitz RSS 队列分配稳定":
      let loop = newEventLoop()
      defer: loop.shutdown()

      let datapath = newHighPerfDatapath(loop, dtIoUring, 4,
        {dfRecvSideScaling, dfRawSocket, dfRecvCoalescing})

      datapath.configureWorkerAffinity(0, [2, 2, 1])
      let binding = datapath.workerBinding(0)
      check binding.workerId == 0
      check binding.cpus == @[1, 2]

      let queue1 = datapath.queueForPacket("10.1.0.1", "10.1.0.2", Port(4433), Port(5555))
      let queue2 = datapath.queueForPacket("10.1.0.1", "10.1.0.2", Port(4433), Port(5555))
      check queue1 == queue2
      let queue3 = datapath.queueForPacket("10.1.0.2", "10.1.0.1", Port(5555), Port(4433))
      if datapath.queueCount > 1:
        check queue1 != queue3

    test "raw datapath 路由数据报并记录快照":
      let loop = newEventLoop()
      defer: loop.shutdown()

      let datapath = newHighPerfDatapath(loop, dtRaw, 2,
        {dfRecvSideScaling, dfRawSocket})

      var received: seq[uint8] = @[]
      let server = loop.bindUdp("127.0.0.1", Port(0),
        UdpEventHandlers(
          onDatagram: proc(endpoint: UdpEndpoint, msg: UdpMessage) {.closure.} =
            received = msg.payload
        ),
        datapathType = dtRaw)
      defer: server.close()

      let client = loop.bindUdp("127.0.0.1", Port(0), datapathType = dtRaw)
      defer: client.close()

      let queue = datapath.attachEndpoint(client, server.localAddress, server.localPort)
      check queue in 0..<datapath.queueCount

      let payload = @[0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8]
      let sendFuture = datapath.routeDatagram(client, server.localAddress, server.localPort, payload)
      check loop.runUntil(sendFuture, 200)

      var spin = 0
      while received.len == 0 and spin < 40:
        discard loop.poll(10)
        inc spin
      check received == payload

      let snapshot = datapath.snapshot()
      check snapshot.datapathType == dtRaw
      check snapshot.queueDepths.len == datapath.queueCount
      check snapshot.queueDepths[queue] >= 1

    test "多队列批量发送统计":
      let loop = newEventLoop()
      defer: loop.shutdown()

      let datapath = newHighPerfDatapath(loop, dtIoUring, 3,
        {dfRecvSideScaling, dfRawSocket},
        maxBatchSize = 4,
        batchFlushIntervalMs = 5)

      var received: seq[seq[uint8]] = @[]
      let server = loop.bindUdp("127.0.0.1", Port(0),
        UdpEventHandlers(
          onDatagram: proc(endpoint: UdpEndpoint, msg: UdpMessage) {.closure.} =
            received.add(msg.payload)
        ),
        datapathType = dtIoUring)
      defer: server.close()

      let client = loop.bindUdp("127.0.0.1", Port(0), datapathType = dtIoUring)
      defer: client.close()

      let queue = datapath.attachEndpoint(client, server.localAddress, server.localPort)
      discard datapath.attachEndpoint(server, client.localAddress, client.localPort)

      var futures: seq[Future[void]] = @[]
      for i in 0..<3:
        let payload = @[uint8(i + 1), 0xAA'u8, 0xBB'u8, 0xCC'u8]
        futures.add(datapath.routeDatagram(client, server.localAddress, server.localPort, payload))

      for fut in futures:
        check loop.runUntil(fut, 200)

      var spin = 0
      while received.len < 3 and spin < 60:
        discard loop.poll(5)
        inc spin
      check received.len == 3

      let stats = datapath.queueStats(Natural(queue))
      check stats.flushedDatagrams >= 3
      check stats.flushedBatches >= 1
      check stats.recvMessages == 3

      let snapshot = datapath.snapshot()
      check snapshot.queuePendingSends[queue] == 0
      check snapshot.queuePendingRecvs[queue] == 0
      check snapshot.totalBatchDatagrams >= 3
      check snapshot.totalRecvMessages == 3
      check loop.runUntil(datapath.flushAll(), 200)
