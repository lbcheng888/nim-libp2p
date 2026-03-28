{.used.}

import testnative, ./pubsub/testpubsub, ./onion/testonionpay, ./onion/testonionpay_path,
  ./coinjoin/testsuite, testlsmr

when defined(libp2p_msquic_experimental):
  import test_qpack_huffman,
    test_msquic_builder,
    test_quic_runtime_info,
    testquicconfig
