import std/[options, unittest]

import "../libp2p/builders"
import "../libp2p/upgrademngrs/muxedupgrade"
import "../libp2p/transports/msquictransport"

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
      check sw.transports.len == 1
      check sw.transports[0] of MsQuicTransport
      check sw.transports[0].upgrader.secureManagers.len == 0
      let upgrader = MuxedUpgrade(sw.transports[0].upgrader)
      check upgrader.muxers.len == 0

    test "legacy withQuicTransport delegates to MsQuic":
      var builder = SwitchBuilder.new()
      builder = builder.withQuicTransport()
      check builder != nil

    test "runtime selection helpers update load policy":
      var cfg = MsQuicTransportBuilderConfig.init()
      cfg.useBuiltinRuntime()
      check cfg.config.loadOptions.builtinPolicy == QuicRuntimeBuiltinPolicy.mbpOnly

      cfg.preferBuiltinRuntime()
      check cfg.config.loadOptions.builtinPolicy == QuicRuntimeBuiltinPolicy.mbpOnly

      cfg.useNativeRuntime()
      check cfg.config.loadOptions.builtinPolicy == QuicRuntimeBuiltinPolicy.mbpOnly

      cfg.useAutoRuntime()
      check cfg.config.loadOptions.builtinPolicy == QuicRuntimeBuiltinPolicy.mbpOnly
else:
  suite "MsQuic switch builder":
    test "experimental feature disabled":
      skip("libp2p_msquic_experimental not enabled")
