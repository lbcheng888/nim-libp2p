{.used.}

import std/[json, os, strutils, times]
import unittest2

import ../libp2p/transports/tsnet/oracle
import ../libp2p/utility

proc fixturePath(name: string): string =
  currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

proc tempOraclePath(tag: string): string =
  let dir = getTempDir() / ("nim-libp2p-tsnet-oracle-" & tag & "-" & $epochTime())
  createDir(dir)
  dir / "fixture.json"

suite "Tsnet oracle fixture":
  test "real self-hosted fixture parses with 901/sin derp data":
    let loaded = loadOracleFixture(fixturePath("self_hosted_901_sin.json"))
    check loaded.isOk()
    check loaded.get().schemaVersion == CurrentTsnetOracleFixtureSchemaVersion
    check loaded.get().source.contains("go-tsnetbridge")
    check loaded.get().status["tailnetDerpMapSummary"].getStr() == "901/sin"
    check loaded.get().derpMap["regions"][0]["regionCode"].getStr() == "sin"
    check loaded.get().ping["ok"].getBool()
    check loaded.get().resolveRemote.kind == JNull

  test "fixture round-trips optional payload families":
    var fixture = TsnetOracleFixture.init()
    fixture.source = "unit-test"
    fixture.capturedAtUnixMilli = 1774333653153
    fixture.tcpListener = %*{"ok": true, "listeningAddress": "127.0.0.1:10002", "port": 10002}
    fixture.tcpDial = %*{"ok": true, "proxyAddress": "127.0.0.1:41001"}
    fixture.udpListener = %*{"ok": true, "listeningAddress": "127.0.0.1:63432", "port": 63432}
    fixture.udpDial = %*{"ok": true, "proxyAddress": "127.0.0.1:42001"}
    fixture.resolveRemote = %*{"ok": true, "remoteAddress": "/ip4/100.64.0.24/tcp/10002/tsnet"}

    let path = tempOraclePath("roundtrip")
    let stored = storeOracleFixture(path, fixture)
    check stored.isOk()

    let loaded = loadOracleFixture(path)
    check loaded.isOk()
    check loaded.get().tcpListener["port"].getInt() == 10002
    check loaded.get().udpDial["proxyAddress"].getStr() == "127.0.0.1:42001"
    check loaded.get().resolveRemote["remoteAddress"].getStr() == "/ip4/100.64.0.24/tcp/10002/tsnet"
