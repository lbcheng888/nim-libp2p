{.used.}

import
  testfloodsub, testgossipsubcontrolmessages, testgossipsubcustomconn,
  testgossipsubfanout, testgossipsubgossip, testgossipsubgossipcompatibility,
  testgossipsubheartbeat, testgossipsubmeshmanagement, testgossipsubmessagecache,
  testgossipsubmessagehandling, testgossipsubscoring, testgossipsubsignatureflags

when defined(libp2p_run_gossipsub_tests):
  import testgossipsubskipmcache
