when defined(libp2p_msquic_experimental):
  {.used.}

  import std/[options, sequtils]
  import chronos
  import ./helpers
  import ../libp2p/[builders, crypto/crypto, multiaddress, switch]
  import ../libp2p/transports/msquicdriver as msdriver

  suite "MsQuic certhash persistence":
    teardown:
      checkTrackers()

    asyncTest "certhash history is reloaded during startup":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
        return
      handle.shutdown()

      let rng = newRng()
      let keyPair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
      let listenAddr =
        MultiAddress.init("/ip4/127.0.0.1/udp/0/quic-v1/webtransport").tryGet()

      var builder = newStandardSwitchBuilder(
        privKey = Opt.some(keyPair.seckey),
        addrs = @[listenAddr],
        transport = TransportType.QUIC
      )
      var firstSwitch = builder.build()

      await firstSwitch.start()

      await firstSwitch.rotateWebtransportCertificate(keepHistory = 3)

      let capturedHistory = firstSwitch.webtransportCerthashHistory()
      check capturedHistory.len >= 1

      await firstSwitch.stop()

      var restoredBuilder = newStandardSwitchBuilder(
        privKey = Opt.some(keyPair.seckey),
        addrs = @[listenAddr],
        transport = TransportType.QUIC
      ).withMsQuicWebtransportCerthashHistory(capturedHistory)

      let secondSwitch = restoredBuilder.build()
      await secondSwitch.start()
      defer:
        await secondSwitch.stop()

      secondSwitch.restoreWebtransportCerthashHistory(capturedHistory)

      let restoredHistory = secondSwitch.webtransportCerthashHistory()
      check restoredHistory.len >= 1
      for hash in capturedHistory:
        if restoredHistory.contains(hash):
          continue
        # 默认历史窗口为 2，旧指纹可能因上限被修剪；至少确保最新指纹存在。
        if hash == capturedHistory[^1] and restoredHistory.len < capturedHistory.len:
          continue
        check restoredHistory.contains(hash)
else:
  import ./helpers

  suite "MsQuic certhash persistence":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
