{.used.}

import std/[sequtils, strutils]
import unittest2

import
  ../libp2p/[
    builders,
    crypto/crypto,
    multiaddress,
    transports/transport,
    transports/tsnettransport,
    upgrademngrs/upgrade,
  ]

import ./helpers, ./commontransport

suite "Tsnet transport":
  teardown:
    checkTrackers()

  proc makeTransport(): Transport =
    let privKey = PrivateKey.random(rng[]).expect("private key")
    TsnetTransport.new(Upgrade(), privKey)

  test "withTsnetTransport accepts custom config":
    var builder = SwitchBuilder.new()
    var cfg = TsnetTransportBuilderConfig.init()
    cfg.hostname = "tsnet-test"
    cfg.controlUrl = "https://headscale.example"
    builder = builder.withTsnetTransport(cfg)
    check builder != nil

  asyncTest "listener advertises /tsnet addresses":
    let transport = TsnetTransport(makeTransport())
    await transport.start(@[MultiAddress.init("/ip4/0.0.0.0/tcp/0/tsnet").tryGet()])
    check transport.addrs.len == 1
    check $transport.addrs[0] != "/ip4/0.0.0.0/tcp/0/tsnet"
    check $transport.addrs[0] == ($transport.addrs[0]).toLowerAscii()
    check ($transport.addrs[0]).endsWith("/tsnet")
    check transport.handles(transport.addrs[0])
    let status = transport.tailnetStatus()
    check status.tailnetIPs.len == 2
    check status.tailnetIPs.anyIt(it.startsWith("100.64."))
    await transport.stop()

  test "handles rejects malformed /tsnet shapes":
    let transport = makeTransport()
    check not transport.handles(MultiAddress.init("/ip4/127.0.0.1/tsnet").tryGet())
    check not transport.handles(MultiAddress.init("/tsnet").tryGet())

  commonTransportTest(makeTransport, "/ip4/0.0.0.0/tcp/0/tsnet")
