{.used.}

# Nim-Libp2p
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import
  testvarint, testconnection, testbridgestream, testminprotobuf, testsemaphore,
  testheartbeat, testfuture, testzeroqueue, testbytesview

import testminasn1, testrsa, testecnist, tested25519, testsecp256k1, testcrypto

import
  testmultibase, testmultihash, testmultiaddress, testcid, testpeerid,
  testsigned_envelope, testrouting_record

import
  testtcptransport,
  testtortransport,
  testwstransport,
  testquic,
  testmemorytransport,
  transports/tls/testcertificate

import
  testnameresolve, testmultistream, testbufferstream, testidentify,
  testobservedaddrmanager, testconnmngr, testswitch, testnoise, testpeerinfo,
  testpeerstore, testping, testmplex, testrelayv1, testyamux,
  testyamuxheader, testautonat, testautonatservice, testautonatv2, testautonatv2service,
  testautorelay, testdcutr, testhpservice, testutility, testhelpers,
  testwildcardresolverservice, testperf, testpkifilter, testpnet, testgossip_optimum,
  testlivestream, testepisub, testfetch, testpeerrecord, testhttp, testgossipsub_limits,
  teststandardservices, testupdater, testresourcemanager, testdatatransfer, testquicconfig,
  integration/connectivity/testdirectconnect, integration/connectivity/testmdnsconnect

when defined(libp2p_run_relay_tests):
  import testrelayv2
import testtls

import discovery/testdiscovery

when defined(libp2p_run_kademlia_tests):
  import kademlia/[testencoding, testroutingtable, testfindnode, testputval, testping]

when defined(libp2p_autotls_support):
  import testautotls

import
  mix/[
    testcrypto, testcurve25519, testtagmanager, testseqnogenerator, testserialization,
    testmixmessage, testsphinx, testmultiaddr, testfragmentation, testmixnode, testconn,
  ]
