# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[times, strutils, tables, json]
import chronos

proc unixNowNs*(): uint64 =
  (epochTime() * 1_000_000_000.0).uint64

proc clampToInt64*(value: uint64): int64 =
  if value > uint64(high(int64)):
    high(int64)
  else:
    value.int64

proc normalizedResourceAttributes*(
    attributes: seq[(string, string)], scopeVersion: string
): seq[(string, string)] =
  var indices = initTable[string, int]()
  var result: seq[(string, string)] = @[]

  proc upsert(key, value: string) =
    let trimmedKey = key.strip()
    if trimmedKey.len == 0:
      return
    let trimmedValue = value.strip()
    let lowered = trimmedKey.toLowerAscii()
    let existingIdx = indices.getOrDefault(lowered, -1)
    if existingIdx >= 0:
      result[existingIdx] = (trimmedKey, trimmedValue)
    else:
      indices[lowered] = result.len
      result.add((trimmedKey, trimmedValue))

  for (key, value) in attributes:
    upsert(key, value)

  if "service.name" notin indices:
    upsert("service.name", "nim-libp2p")

  if "telemetry.sdk.language" notin indices:
    upsert("telemetry.sdk.language", "nim")

  if "telemetry.sdk.name" notin indices:
    upsert("telemetry.sdk.name", "nim-libp2p")

  if scopeVersion.len > 0 and "telemetry.sdk.version" notin indices:
    upsert("telemetry.sdk.version", scopeVersion)

  result

proc buildHeaders*(
    headers: seq[(string, string)], contentType: string = "application/json"
): seq[(string, string)] =
  var indices = initTable[string, int]()
  var finalHeaders: seq[(string, string)] = @[]

  proc upsert(key, value: string) =
    let trimmedKey = key.strip()
    if trimmedKey.len == 0:
      return
    let trimmedValue = value.strip()
    let lowered = trimmedKey.toLowerAscii()
    let existingIdx = indices.getOrDefault(lowered, -1)
    if existingIdx >= 0:
      finalHeaders[existingIdx] = (trimmedKey, trimmedValue)
    else:
      indices[lowered] = finalHeaders.len
      finalHeaders.add((trimmedKey, trimmedValue))

  for (key, value) in headers:
    upsert(key, value)

  if contentType.len > 0:
    let loweredContentType = "content-type"
    if loweredContentType notin indices:
      upsert("Content-Type", contentType)

  finalHeaders

proc makeAttributes*(pairs: seq[(string, string)]): JsonNode =
  var result = newJArray()
  for (key, value) in pairs:
    let trimmedKey = key.strip()
    if trimmedKey.len == 0:
      continue
    var entry = newJObject()
    entry["key"] = %trimmedKey
    var attrValue = newJObject()
    attrValue["stringValue"] = %(value)
    entry["value"] = attrValue
    result.add(entry)
  result

{.pop.}
