{.used.}

import std/[os, strutils]
import unittest2

import ../libp2p/transports/[tsnet/oracle, tsnet/session]
import ../libp2p/utility

proc fixturePath(name: string): string =
  currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

suite "Tsnet session models":
  test "self-hosted oracle fixture parses into typed session snapshot":
    let fixture = loadOracleFixture(fixturePath("self_hosted_901_sin.json"))
    check fixture.isOk()

    let status = parseStatusSnapshot(fixture.get().status)
    check status.isOk()
    check status.get().tailnetDerpMapSummary == "901/sin"
    check status.get().tailnetPeers.len == 1
    check status.get().tailnetPeers[0].relay == "sin"

    let derpMap = parseDerpMapSnapshot(fixture.get().derpMap)
    check derpMap.isOk()
    check derpMap.get().regionCount == 1
    check derpMap.get().regions[0].nodes[0].hostName.contains("sslip.io")

    let ping = parsePingSnapshot(fixture.get().ping)
    check ping.isOk()
    check ping.get().avgLatencyMs == 751

    let snapshot = TsnetSessionSnapshot.init(
      source = fixture.get().source,
      capturedAtUnixMilli = fixture.get().capturedAtUnixMilli,
      status = status.get(),
      derpMap = derpMap.get(),
      ping = ping.get()
    )
    check snapshot.homeDerpLabel() == "901/sin"
    check snapshot.peerCache().len == 1
    check snapshot.peerCache()[0].dnsName == "lbchengdemacbook-pro-local"
