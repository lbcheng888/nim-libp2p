# Configuration loading utilities for the Pebble Nim rewrite.
# Provides layered JSON/INI parsing, environment overrides, and helpers for
# flattening hierarchical values into dot-separated keys.

import std/[json, os, sequtils, strutils, tables]

type
  ConfigError* = object of CatchableError

  ConfigFormat* = enum
    cfgJson

  FlatConfig* = OrderedTable[string, string]

proc newFlatConfig*(): FlatConfig =
  initOrderedTable[string, string]()

proc detectFormat(path: string): ConfigFormat =
  let (_, _, ext) = splitFile(path)
  let extLower = ext.toLowerAscii()
  case extLower
  of ".json":
    cfgJson
  else:
    raise newException(ConfigError, "unsupported config format: " & extLower)

proc flattenJson(into: var FlatConfig; prefix: string; node: JsonNode) =
  case node.kind
  of JObject:
    for k, v in node.pairs:
      let nextPrefix = if prefix.len == 0: k else: prefix & "." & k
      flattenJson(into, nextPrefix, v)
  of JArray:
    for idx, v in node.elems:
      let nextPrefix = if prefix.len == 0: $idx else: prefix & "." & $idx
      flattenJson(into, nextPrefix, v)
  of JNull:
    into[prefix] = ""
  else:
    into[prefix] = node.getStr()

proc loadConfigFile*(path: string): FlatConfig =
  if not path.fileExists():
    raise newException(ConfigError, "config file not found: " & path)
  result = newFlatConfig()
  let format = detectFormat(path)
  case format
  of cfgJson:
    let parsed = parseJson(readFile(path))
    flattenJson(result, "", parsed)

proc mergeConfig*(base: var FlatConfig; overlay: FlatConfig) =
  for key, value in overlay:
    base[key] = value

proc registerDefaults*(base: var FlatConfig; defaults: FlatConfig) =
  for key, value in defaults:
    if not base.hasKey(key):
      base[key] = value

proc applyEnvOverrides*(base: var FlatConfig; prefix = "PEBBLE_") =
  for key, value in envPairs():
    if key.startsWith(prefix):
      let trimmed = key.substr(prefix.len).replace("__", ".").replace("_", ".")
      base[trimmed.toLowerAscii()] = value

proc get*(cfg: FlatConfig; key: string; fallback: string = ""): string =
  if cfg.hasKey(key):
    return cfg[key]
  fallback

proc require*(cfg: FlatConfig; key: string): string =
  if cfg.hasKey(key):
    return cfg[key]
  raise newException(ConfigError, "missing required config key: " & key)

proc toBool*(cfg: FlatConfig; key: string; fallback = false): bool =
  if not cfg.hasKey(key):
    return fallback
  let value = cfg[key].toLowerAscii()
  value in ["1", "true", "yes", "on"]

proc toInt*(cfg: FlatConfig; key: string; fallback: int = 0): int =
  if not cfg.hasKey(key):
    return fallback
  cfg[key].parseInt()

proc toFloat*(cfg: FlatConfig; key: string; fallback: float = 0.0): float =
  if not cfg.hasKey(key):
    return fallback
  cfg[key].parseFloat()
