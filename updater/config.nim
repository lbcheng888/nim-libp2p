## nim-libp2p mobile updater configuration loader

import std/[json, os, sequtils, strformat, strutils]
import pkg/results
import chronos

type
  UpdaterConfig* = object
    listenAddrs*: seq[string]
    bootstrapPeers*: seq[string]
    manifestTopic*: string
    ipnsNames*: seq[string]
    fallbackKeys*: seq[string]
    ipnsPollInterval*: Duration
    agentVersion*: string
    preferIpns*: bool

proc defaultUpdaterConfig*(): UpdaterConfig =
  UpdaterConfig(
    listenAddrs: @["/ip4/0.0.0.0/tcp/0"],
    bootstrapPeers: @[],
    manifestTopic: "unimaker/mobile/version",
    ipnsNames: @[],
    fallbackKeys: @[],
    ipnsPollInterval: chronos.seconds(30),
    agentVersion: "nim-updater/0.1.0",
    preferIpns: true,
  )

proc readStringArray(node: JsonNode, key: string): seq[string] =
  if not node.hasKey(key):
    return @[]
  let arr = node[key]
  if arr.kind != JArray:
    raise newException(ValueError, fmt"config field `{key}` must be an array of strings")
  for item in arr.items:
    if item.kind != JString:
      raise newException(ValueError, fmt"config field `{key}` may only contain strings")
    result.add(item.getStr())

proc loadUpdaterConfig*(path: string): Result[UpdaterConfig, string] =
  var config = defaultUpdaterConfig()

  if path.len == 0:
    return ok(config)

  let raw =
    try:
      readFile(path)
    except OSError as exc:
      return err("failed to read config: " & exc.msg)

  let root =
    try:
      parseJson(raw)
    except JsonParsingError as exc:
      return err("failed to parse config JSON: " & exc.msg)

  if root.kind != JObject:
    return err("config file root must be a JSON object")

  try:
    let listeners = readStringArray(root, "listen")
    if listeners.len > 0:
      config.listenAddrs = listeners

    let bootstrap = readStringArray(root, "bootstrap")
    if bootstrap.len > 0:
      config.bootstrapPeers = bootstrap

    let ipns = readStringArray(root, "ipns")
    if ipns.len > 0:
      config.ipnsNames = ipns

    let fallback = readStringArray(root, "fallbackKeys")
    if fallback.len > 0:
      config.fallbackKeys = fallback

    if root.hasKey("manifestTopic"):
      config.manifestTopic = root["manifestTopic"].getStr()
    if root.hasKey("agentVersion"):
      config.agentVersion = root["agentVersion"].getStr()
    if root.hasKey("ipnsPollSeconds"):
      let rawInterval = root["ipnsPollSeconds"].getInt()
      let clamped = if rawInterval <= 0: 10 else: rawInterval
      config.ipnsPollInterval = chronos.seconds(clamped)
    elif root.hasKey("ipnsPollMillis"):
      let rawMs = root["ipnsPollMillis"].getInt()
      let clamped = if rawMs <= 0: 5000 else: rawMs
      config.ipnsPollInterval = chronos.milliseconds(clamped)
    if root.hasKey("preferIpns"):
      config.preferIpns = root["preferIpns"].getBool()
  except ValueError as exc:
    return err("failed to parse config: " & exc.msg)

  if config.manifestTopic.len == 0:
    return err("manifestTopic must not be empty")

  if config.listenAddrs.len == 0:
    config.listenAddrs = @["/ip4/0.0.0.0/tcp/0"]

  ok(config)
