{.used.}

# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

when defined(libp2p_msquic_experimental):
  import chronos
  import unittest2
  import pkg/results

  import ../libp2p/transports/msquictransport
  import ../libp2p/multistream
  import ../libp2p/upgrademngrs/upgrade
  import ../libp2p/crypto/crypto

  suite "Quic transport configuration":
    test "webtransport path, query, and draft setters sanitize inputs":
      var rngRef = newRng()
      require not rngRef.isNil
      var rngCtx = rngRef[]
      let keyPair = KeyPair.random(rngCtx).expect("keypair")
      let privateKey = keyPair.seckey

      let ms = MultistreamSelect(handlers: @[], codec: Codec, resourceManager: nil)
      let upgrade = Upgrade(ms: ms, secureManagers: @[])
      let transport = newMsQuicTransport(upgrade, privateKey)

      transport.setWebtransportQuery("")
      transport.setWebtransportPath("/.well-known/libp2p-webtransport")

      transport.setWebtransportPath("metrics")
      check transport.webtransportPath() == "/metrics"
      check transport.webtransportQuery() == ""

      transport.setWebtransportQuery("v=1")
      check transport.webtransportQuery() == "?v=1"
      check transport.webtransportRequestTarget() == "/metrics?v=1"

      transport.setWebtransportPath("/api/data?foo=bar")
      check transport.webtransportPath() == "/api/data"
      check transport.webtransportQuery() == "?foo=bar"
      check transport.webtransportRequestTarget() == "/api/data?foo=bar"

      transport.setWebtransportQuery("?")
      check transport.webtransportQuery() == ""
      check transport.webtransportRequestTarget() == "/api/data"

      transport.setWebtransportDraft(" draft-02 ")
      check transport.webtransportDraft() == "draft-02"

    test "0-RTT policy remains opt-in and visible in stats":
      var rngRef = newRng()
      require not rngRef.isNil
      var rngCtx = rngRef[]
      let keyPair = KeyPair.random(rngCtx).expect("keypair")
      let privateKey = keyPair.seckey

      let ms = MultistreamSelect(handlers: @[], codec: Codec, resourceManager: nil)
      let upgrade = Upgrade(ms: ms, secureManagers: @[])
      let transport = newMsQuicTransport(upgrade, privateKey)

      let coldStats = collectMsQuicTransportStats(transport)
      check not coldStats.zeroRttEnabled
      check not coldStats.resumptionTicketConfigured

      transport.setTlsResumptionTicket(@[1'u8, 2'u8, 3'u8, 4'u8])
      transport.setTlsZeroRttEnabled(true)
      transport.setTlsZeroRttAllowedAlpns(@["libp2p"])

      let warmStats = collectMsQuicTransportStats(transport)
      check warmStats.zeroRttEnabled
      check warmStats.resumptionTicketConfigured
      check "libp2p" in warmStats.zeroRttAllowedAlpns
else:
  import unittest2

  suite "Quic transport configuration":
    test "MsQuic transport disabled":
      checkpoint("QUIC configuration requires -d:libp2p_msquic_experimental")
      skip()
