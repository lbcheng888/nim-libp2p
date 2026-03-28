{.used.}

import std/[json, os, strutils, times]
import unittest2

import ../libp2p/transports/tsnet/state
import ../libp2p/utility

proc tempStateDir(tag: string): string =
  let path = getTempDir() / ("nim-libp2p-tsnet-state-" & tag & "-" & $epochTime())
  createDir(path)
  path

suite "Tsnet stored state":
  test "state round-trips through Nim JSON persistence":
    let dir = tempStateDir("roundtrip")
    var state = TsnetStoredState.init(
      hostname = "nim-mac-rerun6",
      controlUrl = "https://64-176-84-12.sslip.io"
    )
    state.machineKey = "privkey:1111111111111111111111111111111111111111111111111111111111111111"
    state.nodeKey = "privkey:2222222222222222222222222222222222222222222222222222222222222222"
    state.wgKey = "privkey:3333333333333333333333333333333333333333333333333333333333333333"
    discard syncDerivedPublicKeys(state)
    state.tailnetIPs = @["100.64.0.25", "fd7a:115c:a1e0::19"]
    state.controlPublicKey = "mkey:public"
    state.controlLegacyPublicKey = "mkey:legacy"
    state.nodeId = "node-1"
    state.userLogin = "lbcheng"
    state.homeDerp = "901/sin"
    state.lastControlSuccessUnixMilli = 1774333653153
    state.lastRegisterAttemptUnixMilli = 1774333653200
    state.lastMapPollAttemptUnixMilli = 1774333653300
    state.lastControlBootstrapError = "register pending"
    state.peerCache = @[
      TsnetStoredPeer(
        nodeId: "peer-1",
        hostName: "nim-android-rerun6",
        dnsName: "nim-android-rerun6",
        relay: "sin",
        tailscaleIPs: @["100.64.0.25", "fd7a:115c:a1e0::19"],
        allowedIPs: @["100.64.0.25/32"],
        lastHandshakeUnixMilli: 1774333693551
      )
    ]

    let stored = storeStoredState(dir, state)
    check stored.isOk()
    check stored.get().endsWith(TsnetStoredStateFilename)

    let loaded = loadStoredState(dir)
    check loaded.isOk()
    check loaded.get().schemaVersion == CurrentTsnetStoredStateSchemaVersion
    check loaded.get().hostname == "nim-mac-rerun6"
    check loaded.get().controlUrl == "https://64-176-84-12.sslip.io"
    check loaded.get().controlPublicKey == "mkey:public"
    check loaded.get().controlLegacyPublicKey == "mkey:legacy"
    check loaded.get().machinePublicKey.startsWith(TsnetMachinePublicKeyPrefix)
    check loaded.get().nodePublicKey.startsWith(TsnetNodePublicKeyPrefix)
    check loaded.get().discoPublicKey.startsWith(TsnetDiscoPublicKeyPrefix)
    check loaded.get().homeDerp == "901/sin"
    check loaded.get().tailnetIPs.len == 2
    check loaded.get().lastControlSuccessUnixMilli == 1774333653153
    check loaded.get().lastRegisterAttemptUnixMilli == 1774333653200
    check loaded.get().lastMapPollAttemptUnixMilli == 1774333653300
    check loaded.get().lastControlBootstrapError == "register pending"
    check loaded.get().peerCache.len == 1
    check loaded.get().peerCache[0].tailscaleIPs[0] == "100.64.0.25"
    check loaded.get().updatedAtUnixMilli >= loaded.get().createdAtUnixMilli

  test "missing fields recover to defaults":
    let dir = tempStateDir("defaults")
    let path = dir / TsnetStoredStateFilename
    writeFile(path, """{"hostname":"nim-state-only"}""")

    let loaded = loadStoredState(dir)
    check loaded.isOk()
    check loaded.get().schemaVersion == CurrentTsnetStoredStateSchemaVersion
    check loaded.get().hostname == "nim-state-only"
    check loaded.get().controlUrl == ""
    check loaded.get().peerCache.len == 0
    check loaded.get().createdAtUnixMilli > 0

  test "damaged state file fails fast":
    let dir = tempStateDir("broken")
    let path = dir / TsnetStoredStateFilename
    writeFile(path, """{"hostname":""")

    let loaded = loadStoredState(dir)
    check loaded.isErr()
    check loaded.error.contains("failed to load tsnet state")

  test "identity keys are generated once and preserved across persistence":
    let dir = tempStateDir("identity")
    var state = TsnetStoredState.init(
      hostname = "nim-generated",
      controlUrl = "https://headscale.example"
    )

    let generated = ensureIdentityKeys(state)
    check generated.isOk()
    check generated.get()
    check state.machineKey.startsWith(TsnetMachineKeyPrefix)
    check state.nodeKey.startsWith(TsnetNodeKeyPrefix)
    check state.wgKey.startsWith(TsnetWireGuardKeyPrefix)
    check state.machinePublicKey.startsWith(TsnetMachinePublicKeyPrefix)
    check state.nodePublicKey.startsWith(TsnetNodePublicKeyPrefix)
    check state.discoPublicKey.startsWith(TsnetDiscoPublicKeyPrefix)

    let machineKey = state.machineKey
    let nodeKey = state.nodeKey
    let wgKey = state.wgKey
    let machinePublicKey = state.machinePublicKey
    let nodePublicKey = state.nodePublicKey
    let discoPublicKey = state.discoPublicKey

    let stored = storeStoredState(dir, state)
    check stored.isOk()

    var loaded = loadStoredState(dir).valueOr:
      checkpoint error
      check false
      return
    let regenerated = ensureIdentityKeys(loaded)
    check regenerated.isOk()
    check not regenerated.get()
    check loaded.machineKey == machineKey
    check loaded.nodeKey == nodeKey
    check loaded.wgKey == wgKey
    check loaded.machinePublicKey == machinePublicKey
    check loaded.nodePublicKey == nodePublicKey
    check loaded.discoPublicKey == discoPublicKey
