{.push raises: [].}

import std/[json, os]

import ../../utility

type
  TsnetOracleFixture* = object
    schemaVersion*: int
    source*: string
    capturedAtUnixMilli*: int64
    controlRegister*: JsonNode
    controlMapPoll*: JsonNode
    status*: JsonNode
    derpMap*: JsonNode
    ping*: JsonNode
    tcpListener*: JsonNode
    tcpDial*: JsonNode
    udpListener*: JsonNode
    udpDial*: JsonNode
    resolveRemote*: JsonNode

const
  CurrentTsnetOracleFixtureSchemaVersion* = 1
  TsnetOracleFixtureFilename* = "nim-tsnet-oracle.json"

proc fixtureField(node: JsonNode, key: string): JsonNode =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc jsonString(node: JsonNode, key: string, defaultValue = ""): string =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return defaultValue
  let value = node.getOrDefault(key)
  if value.kind == JString:
    return value.getStr()
  defaultValue

proc jsonInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return defaultValue
  let value = node.getOrDefault(key)
  case value.kind
  of JInt:
    value.getBiggestInt().int64
  of JFloat:
    value.getFloat().int64
  else:
    defaultValue

proc init*(_: type[TsnetOracleFixture]): TsnetOracleFixture =
  TsnetOracleFixture(
    schemaVersion: CurrentTsnetOracleFixtureSchemaVersion,
    source: "",
    capturedAtUnixMilli: 0,
    controlRegister: newJNull(),
    controlMapPoll: newJNull(),
    status: newJNull(),
    derpMap: newJNull(),
    ping: newJNull(),
    tcpListener: newJNull(),
    tcpDial: newJNull(),
    udpListener: newJNull(),
    udpDial: newJNull(),
    resolveRemote: newJNull()
  )

proc toJson*(fixture: TsnetOracleFixture): JsonNode =
  result = newJObject()
  result["schemaVersion"] = %fixture.schemaVersion
  result["source"] = %fixture.source
  result["capturedAtUnixMilli"] = %fixture.capturedAtUnixMilli
  result["controlRegister"] = fixture.controlRegister
  result["controlMapPoll"] = fixture.controlMapPoll
  result["status"] = fixture.status
  result["derpMap"] = fixture.derpMap
  result["ping"] = fixture.ping
  result["tcpListener"] = fixture.tcpListener
  result["tcpDial"] = fixture.tcpDial
  result["udpListener"] = fixture.udpListener
  result["udpDial"] = fixture.udpDial
  result["resolveRemote"] = fixture.resolveRemote

proc parseOracleFixture*(node: JsonNode): Result[TsnetOracleFixture, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet oracle fixture must contain a JSON object")
  var fixture = TsnetOracleFixture(
    schemaVersion: jsonInt64(node, "schemaVersion").int,
    source: jsonString(node, "source"),
    capturedAtUnixMilli: jsonInt64(node, "capturedAtUnixMilli"),
    controlRegister: fixtureField(node, "controlRegister"),
    controlMapPoll: fixtureField(node, "controlMapPoll"),
    status: fixtureField(node, "status"),
    derpMap: fixtureField(node, "derpMap"),
    ping: fixtureField(node, "ping"),
    tcpListener: fixtureField(node, "tcpListener"),
    tcpDial: fixtureField(node, "tcpDial"),
    udpListener: fixtureField(node, "udpListener"),
    udpDial: fixtureField(node, "udpDial"),
    resolveRemote: fixtureField(node, "resolveRemote")
  )
  if fixture.schemaVersion <= 0:
    fixture.schemaVersion = CurrentTsnetOracleFixtureSchemaVersion
  ok(fixture)

proc loadOracleFixture*(path: string): Result[TsnetOracleFixture, string] =
  if path.len == 0:
    return err("tsnet oracle fixture path is empty")
  if not fileExists(path):
    return err("tsnet oracle fixture does not exist: " & path)
  try:
    let parsed = parseJson(readFile(path))
    parseOracleFixture(parsed)
  except CatchableError as exc:
    err("failed to load tsnet oracle fixture from " & path & ": " & exc.msg)

proc oraclePath*(stateDir: string): string =
  if stateDir.len == 0:
    return ""
  stateDir / TsnetOracleFixtureFilename

proc storeOracleFixture*(path: string, fixture: TsnetOracleFixture): Result[void, string] =
  if path.len == 0:
    return err("tsnet oracle fixture path is empty")
  try:
    createDir(path.parentDir())
    writeFile(path, $fixture.toJson())
    ok()
  except CatchableError as exc:
    err("failed to store tsnet oracle fixture to " & path & ": " & exc.msg)
