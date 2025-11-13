import std/[options, unittest]

import "../libp2p/builders"

when defined(libp2p_msquic_experimental):
  suite "MsQuic switch builder":
    test "withMsQuicTransport accepts custom config":
      var builder = SwitchBuilder.new()
      var cfg = MsQuicTransportBuilderConfig.init()
      cfg.config.alpns = @["libp2p-test"]
      builder = builder.withMsQuicTransport(cfg)
      check builder != nil

    test "newStandardSwitchBuilder builds with QUIC transport":
      let switchBuilder = newStandardSwitchBuilder(transport = TransportType.QUIC)
      let sw = switchBuilder.build()
      check sw != nil

    test "legacy withQuicTransport delegates to MsQuic":
      var builder = SwitchBuilder.new()
      builder = builder.withQuicTransport()
      check builder != nil
else:
  suite "MsQuic switch builder":
    test "experimental feature disabled":
      skip("libp2p_msquic_experimental not enabled")
