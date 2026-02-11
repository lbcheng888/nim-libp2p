when defined(libp2p_run_rendezvous_tests):
  include "testrendezvousinterface_enabled.nim"
else:
  import ../helpers
  suite "RendezVous Interface":
    test "Rendezvous tests disabled":
      skip()
