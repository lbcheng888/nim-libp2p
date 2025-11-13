when defined(libp2p_run_quic_tests):
  include "testquic_enabled.nim"
else:
  import ./helpers
  suite "Quic transport":
    test "Quic integration tests disabled":
      skip()
