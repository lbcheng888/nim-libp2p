{.used.}

when defined(libp2p_skip_switch_tests):
  import unittest2

  suite "Switch":
    test "skipped by config":
      skip()
elif defined(libp2p_run_switch_tests):
  # Nim-Libp2p
  # (原有 Switch e2e 测试保留在此分支，仅在显式开启时运行)
  include "testswitch_full.nim"
else:
  import unittest2

  suite "Switch":
    test "disabled":
      skip()
