{.used.}

import std/[json, strutils]
import unittest2

import ../libp2p/transports/tsnet/control
import ../libp2p/utility

suite "Tsnet control helpers":
  test "control key URL appends capability version":
    check controlKeyUrl("https://64-176-84-12.sslip.io", 130) ==
      "https://64-176-84-12.sslip.io/key?v=130"
    check controlKeyUrl(" https://64-176-84-12.sslip.io/ ", 109) ==
      "https://64-176-84-12.sslip.io/key?v=109"
    check controlNoiseUrl("https://64-176-84-12.sslip.io/") ==
      "https://64-176-84-12.sslip.io/ts2021"
    check controlRegisterUrl("https://64-176-84-12.sslip.io/") ==
      "https://64-176-84-12.sslip.io/machine/register"
    check controlMapUrl("https://64-176-84-12.sslip.io/") ==
      "https://64-176-84-12.sslip.io/machine/map"

  test "control key payload parses expected fields":
    let payload = %*{
      "legacyPublicKey": "mkey:0000000000000000000000000000000000000000000000000000000000000000",
      "publicKey": "mkey:6056f44267059a3eeab7893cbdfe314d74a5debb0cd63279f8e54930366efc3b",
    }

    let parsed = parseControlServerKey(payload, capabilityVersion = 109, fetchedAtUnixMilli = 1774333653153)
    check parsed.isOk()
    check parsed.get().capabilityVersion == 109
    check parsed.get().publicKey == "mkey:6056f44267059a3eeab7893cbdfe314d74a5debb0cd63279f8e54930366efc3b"
    check parsed.get().legacyPublicKey.startsWith("mkey:")
    check parsed.get().fetchedAtUnixMilli == 1774333653153

  test "control key payload rejects missing public key":
    let parsed = parseControlServerKey(%* {"legacyPublicKey": "mkey:legacy"})
    check parsed.isErr()
    check parsed.error.contains("publicKey")

  test "register and map request builders expose expected protocol shape":
    let registerReq =
      buildRegisterRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:abc",
        machinePublicKey = "mkey:def",
        hostname = "nim-runtime",
        authKey = "tskey-test"
      )
    check registerReq["Version"].getInt() == 130
    check registerReq["NodeKey"].getStr() == "nodekey:abc"
    check registerReq["Auth"]["AuthKey"].getStr() == "tskey-test"
    check registerReq["Hostinfo"]["Hostname"].getStr() == "nim-runtime"

    let mapReq =
      buildMapRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:abc",
        discoPublicKey = "discokey:xyz",
        hostname = "nim-runtime"
      )
    check mapReq["Version"].getInt() == 130
    check mapReq["NodeKey"].getStr() == "nodekey:abc"
    check mapReq["DiscoKey"].getStr() == "discokey:xyz"
    check mapReq["KeepAlive"].getBool()
    check mapReq["Stream"].getBool()
    check mapReq["Compress"].getStr() == "zstd"

  test "register and map response helpers parse expected fields":
    let registerResp = parseRegisterResponse(%*{
      "MachineAuthorized": true,
      "NodeKeyExpired": false,
      "AuthURL": "https://login.example",
      "User": {"LoginName": "lbcheng", "DisplayName": "LB Cheng"},
      "Login": {"LoginName": "lbcheng@example.com"},
      "NodeKeySignature": "present"
    })
    check registerResp.isOk()
    check registerResp.get().machineAuthorized
    check registerResp.get().authUrl == "https://login.example"
    check registerResp.get().userLoginName == "lbcheng"
    check registerResp.get().loginName == "lbcheng@example.com"
    check registerResp.get().nodeKeySignaturePresent

    let mapResp = parseMapResponseSummary(%*{
      "KeepAlive": false,
      "Node": {"ID": 1},
      "DERPMap": {"Regions": {"901": {"RegionCode": "sin"}}},
      "Peers": [{"ID": 2}, {"ID": 3}],
      "Domain": "example.com",
      "CollectServices": true,
      "MapSessionHandle": "session-1",
      "Seq": 42
    })
    check mapResp.isOk()
    check not mapResp.get().keepAlive
    check mapResp.get().nodePresent
    check mapResp.get().derpMapPresent
    check mapResp.get().peerCount == 2
    check mapResp.get().domain == "example.com"
    check mapResp.get().collectServices
    check mapResp.get().mapSessionHandle == "session-1"
    check mapResp.get().seq == 42

  test "bootstrap control returns auth_required stage without auth key":
    let fetchProc: TsnetControlKeyFetchProc =
      proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
        check controlUrl == "https://64-176-84-12.sslip.io"
        ok(TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        ))
    let transport =
      TsnetControlTransport.init(
        fetchServerKeyProc = fetchProc
      )
    let input = TsnetControlBootstrapInput(
      controlUrl: "https://64-176-84-12.sslip.io",
      hostname: "nim-runtime",
      authKey: "",
      wireguardPort: 41641,
      machineKeyPresent: true,
      machinePublicKey: "mkey:machine",
      nodeKeyPresent: true,
      nodePublicKey: "nodekey:node",
      wgKeyPresent: true,
      discoPublicKey: "discokey:wg",
      persistedNodeIdPresent: false
    )
    let boot = bootstrapControl(transport, input)
    check boot.isOk()
    check boot.get().stage == TsnetControlBootstrapStage.AuthRequired
    check boot.get().snapshot.available()
    check boot.get().snapshot.toJson()["register"]["request"]["NodeKey"].getStr() == "nodekey:node"
    check boot.get().snapshot.toJson()["mapPoll"]["request"]["DiscoKey"].getStr() == "discokey:wg"

  test "bootstrap control returns control_reachable stage when auth or node id exists":
    let fetchProc: TsnetControlKeyFetchProc =
      proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
        ok(TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        ))
    let transport =
      TsnetControlTransport.init(
        fetchServerKeyProc = fetchProc
      )
    let input = TsnetControlBootstrapInput(
      controlUrl: "https://64-176-84-12.sslip.io",
      hostname: "nim-runtime",
      authKey: "tskey-test",
      wireguardPort: 41641,
      machineKeyPresent: true,
      machinePublicKey: "mkey:machine",
      nodeKeyPresent: true,
      nodePublicKey: "nodekey:node",
      wgKeyPresent: true,
      discoPublicKey: "discokey:wg",
      persistedNodeIdPresent: false
    )
    let boot = bootstrapControl(transport, input)
    check boot.isOk()
    check boot.get().stage == TsnetControlBootstrapStage.ControlReachable
    check boot.get().snapshot.toJson()["probe"]["protocol"].getStr() == "ts2021/http2"

  test "bootstrap control advances to mapped when register and map transport hooks are present":
    let fetchProc: TsnetControlKeyFetchProc =
      proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
        ok(TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        ))
    let registerProc: TsnetControlRegisterProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        if controlUrl != "https://64-176-84-12.sslip.io":
          return err("unexpected controlUrl")
        let nodeKeyNode = request{"NodeKey"}
        if nodeKeyNode.isNil:
          return err("missing NodeKey")
        if nodeKeyNode.getStr() != "nodekey:node":
          return err("unexpected NodeKey")
        ok(%*{
          "MachineAuthorized": true,
          "User": {"LoginName": "lbcheng", "DisplayName": "LB Cheng"},
          "Login": {"LoginName": "lbcheng@example.com"}
        })
    let mapProc: TsnetControlMapPollProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        let discoKeyNode = request{"DiscoKey"}
        if discoKeyNode.isNil:
          return err("missing DiscoKey")
        if discoKeyNode.getStr() != "discokey:wg":
          return err("unexpected DiscoKey")
        ok(%*{
          "MapSessionHandle": "session-1",
          "Seq": 7,
          "Node": {
            "ID": 1,
            "StableID": "node-1",
            "Name": "nim-runtime",
            "HomeDERP": 901,
            "Addresses": ["100.64.0.10/32", "fd7a:115c:a1e0::10/128"],
            "Hostinfo": {"Hostname": "nim-runtime"}
          },
          "DERPMap": {
            "OmitDefaultRegions": true,
            "Regions": {
              "901": {
                "RegionID": 901,
                "RegionCode": "sin",
                "RegionName": "Vultr Singapore",
                "Nodes": [{
                  "Name": "901a",
                  "RegionID": 901,
                  "HostName": "64-176-84-12.sslip.io",
                  "DERPPort": 443
                }]
              }
            }
          },
          "Peers": [{
            "ID": 2,
            "StableID": "node-2",
            "Name": "peer-runtime",
            "HomeDERP": 901,
            "Addresses": ["100.64.0.11/32"],
            "Hostinfo": {"Hostname": "peer-runtime"}
          }]
        })
    let transport =
      TsnetControlTransport.init(
        fetchServerKeyProc = fetchProc,
        registerNodeProc = registerProc,
        mapPollProc = mapProc
      )
    let input = TsnetControlBootstrapInput(
      controlUrl: "https://64-176-84-12.sslip.io",
      hostname: "nim-runtime",
      authKey: "tskey-test",
      wireguardPort: 41641,
      machineKeyPresent: true,
      machinePublicKey: "mkey:machine",
      nodeKeyPresent: true,
      nodePublicKey: "nodekey:node",
      wgKeyPresent: true,
      discoPublicKey: "discokey:wg",
      persistedNodeIdPresent: false
    )
    let boot = bootstrapControl(transport, input)
    check boot.isOk()
    check boot.get().stage == TsnetControlBootstrapStage.Mapped
    check boot.get().registerResult.snapshot.machineAuthorized
    check boot.get().mapPollResult.summary.mapSessionHandle == "session-1"
    check boot.get().snapshot.toJson()["register"]["response"]["MachineAuthorized"].getBool()
    check boot.get().snapshot.toJson()["mapPoll"]["response"]["MapSessionHandle"].getStr() == "session-1"
