when defined(libp2p_run_perf_tests):
  include "testperf_enabled.nim"
else:
  import ./helpers
  suite "Perf protocol":
    test "Perf tests disabled":
      skip()
