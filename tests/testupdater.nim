{.used.}

# Nim-Libp2p
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import std/[json, os, strutils, tables, tempfiles]
import pkg/results
import unittest2
import stew/byteutils

import ../updater/[config, manifest]

proc writeTempConfig(content: string): string =
  let path = genTempPath("nim-libp2p-updater", ".json")
  writeFile(path, content)
  path

suite "Updater configuration parsing":
  test "returns defaults when no config file provided":
    let res = loadUpdaterConfig("")
    require res.isOk()
    let cfg = res.get()
    check cfg.manifestTopic == "unimaker/mobile/version"
    check cfg.listenAddrs.len == 1
    check cfg.preferIpns

  test "rejects empty manifest topic":
    let path = writeTempConfig("""{"manifestTopic": ""}""")
    defer: discard tryRemoveFile(path)
    let res = loadUpdaterConfig(path)
    check res.isErr()
    check res.error.contains("manifestTopic must not be empty")

  test "rejects non-string entries in string array fields":
    let path = writeTempConfig("""{"listen": ["ok", 42]}""")
    defer: discard tryRemoveFile(path)
    let res = loadUpdaterConfig(path)
    check res.isErr()
    check res.error.contains("config field `listen` may only contain strings")

suite "Updater manifest decoding":
  test "decodes well formed manifest":
    let manifestNode = %*{
      "channel": "stable",
      "version": "1.2.3",
      "sequence": 4,
      "publishedAt": "2025-03-15T10:00:00Z",
      "manifestCid": "bafybeigdyrzt",
      "previousCid": "bafybeiaolder",
      "artifacts": [
        {
          "platform": "android-arm64",
          "kind": "bundle",
          "cid": "bafybeihash1",
          "sizeBytes": 1024,
          "hash": "sha256-abc",
          "url": "https://example.invalid/bundle.tgz",
          "metadata": {"build": "2025.03.15"}
        }
      ],
      "metadata": {"channel": "stable"}
    }
    let res = Manifest.decode(($manifestNode).toBytes())
    require res.isOk()
    let man = res.get()
    check man.channel == "stable"
    check man.sequence == 4
    check man.artifacts.len == 1
    check man.artifacts[0].metadata["build"] == "2025.03.15"
    check man.metadata["channel"] == "stable"

  test "fails when mandatory manifest field missing":
    let manifestNode = %*{
      "version": "1.0.0",
      "sequence": 1,
      "publishedAt": "2025-03-15T10:00:00Z",
      "manifestCid": "bafybeigdemo",
      "artifacts": []
    }
    let res = Manifest.decode(($manifestNode).toBytes())
    check res.isErr()
    check $res.error == "manifest field `channel` missing"

  test "checkValid enforces channel and sequence rules":
    var signed = SignedManifest(
      data: Manifest(
        channel: "",
        version: "1.0.0",
        sequence: 0,
        publishedAt: "2025-03-15T10:00:00Z",
        manifestCid: "bafybeigdemo",
        previousCid: "",
        artifacts: @[],
        metadata: initTable[string, string](),
      )
    )
    check signed.checkValid().isErr()

    signed.data.channel = "stable"
    signed.data.sequence = 1
    check signed.checkValid().isOk()
