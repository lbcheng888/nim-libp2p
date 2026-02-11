when defined(libp2p_run_kademlia_tests):
  include "testfindnode_enabled.nim"
else:
  import ../helpers
  suite "KadDHT - FindNode":
    test "KadDHT find-node tests disabled":
      skip()
