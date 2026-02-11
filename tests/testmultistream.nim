when defined(libp2p_run_multistream_tests):
  include "testmultistream_enabled.nim"
else:
  import ./helpers
  suite "Multistream select":
    test "Multistream integration tests disabled":
      skip()
