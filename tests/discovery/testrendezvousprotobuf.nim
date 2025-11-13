when defined(libp2p_run_rendezvous_tests):
  include "testrendezvousprotobuf_enabled.nim"
else:
  import ../helpers
  suite "RendezVous Protobuf":
    test "Rendezvous protobuf tests disabled":
      skip()
