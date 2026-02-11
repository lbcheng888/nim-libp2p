when defined(libp2p_run_gossipsub_tests):
  include "testpubsubintegration_enabled.nim"
else:
  import ../../helpers
  suite "GossipSub Integration":
    test "GossipSub integration tests disabled":
      skip()
