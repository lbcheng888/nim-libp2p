when defined(libp2p_run_gossipsub_tests):
  include "testgossipsubskipmcache_enabled.nim"
else:
  import ../../helpers
  suite "GossipSub Integration - Skip MCache Support":
    test "GossipSub skip mcache tests disabled":
      skip()
