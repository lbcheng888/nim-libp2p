{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, net, os, sequtils, sets, strformat, strutils, syncio]
from std/nativesockets import selectRead
from std/times import epochTime

import ../../libp2p/crypto/crypto
import ../../libp2p/peerid
import ./libnimlibp2p

type
  HarnessRole = enum
    hrInitiator
    hrResponder

  RemoteInfoServer = object
    socket: Socket
    active: bool

  SignalListener = object
    socket: Socket
    active: bool

  SignalChannel = object
    socket: Socket
    connected: bool

  HarnessOptions = object
    role: HarnessRole
    label: string
    controlUrl: string
    controlProtocol: string
    controlEndpoint: string
    relayEndpoint: string
    authKey: string
    hostname: string
    stateDir: string
    dataDir: string
    bridgeLibraryPath: string
    identityPath: string
    localInfoPath: string
    remoteInfoPath: string
    resultPath: string
    waitTimeoutSec: int
    connectTimeoutMs: int
    wireguardPort: int
    listenPort: int
    logLevel: string
    enableDebug: bool
    dmText: string
    pingSamples: int
    noFileOutput: bool
    emitLocalInfoStdout: bool
    emitResultStdout: bool
    emitLocalInfoTcpHost: string
    emitLocalInfoTcpPort: int
    emitResultTcpHost: string
    emitResultTcpPort: int
    signalHost: string
    signalPort: int
    remoteInfoJson: string
    remoteInfoJsonBase64: string
    remoteInfoStdin: bool
    remoteInfoTcpHost: string
    remoteInfoTcpPort: int
    quicOnly: bool

const
  LocalInfoStdoutPrefix = "TSNET_LOCAL_INFO_JSON_B64="
  ResultStdoutPrefix = "TSNET_RESULT_JSON_B64="
  RemoteInfoStdinPrefix = "TSNET_REMOTE_INFO_JSON_B64="
  HarnessTcpProtocolVersion = 1

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc fail(msg: string) {.noreturn.} =
  raise newException(IOError, msg)

proc takeCStringAndFree(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

template safeCStringExpr(valueExpr: untyped): string =
  block:
    try:
      let value = valueExpr
      takeCStringAndFree(value)
    except CatchableError:
      ""

proc parseJsonSafe(text: string; fallback = "{}"): JsonNode =
  try:
    if text.len == 0:
      return parseJson(fallback)
    parseJson(text)
  except CatchableError:
    parseJson(fallback)

template safeJsonExpr(valueExpr: untyped, fallback = "{}"): JsonNode =
  block:
    try:
      let value = valueExpr
      parseJsonSafe(takeCStringAndFree(value), fallback)
    except CatchableError:
      parseJson(fallback)

proc writeJsonFile(path: string; payload: JsonNode) =
  if path.len == 0:
    return
  let parent = splitFile(path).dir
  if parent.len > 0 and not dirExists(parent):
    createDir(parent)
  writeFile(path, $payload)

proc emitTaggedJson(prefix: string; payload: JsonNode) =
  stdout.writeLine(prefix & base64.encode($payload))
  flushFile(stdout)

proc signalTrace(message: string) =
  try:
    stderr.writeLine("[tsnet-harness-signal] " & message)
    flushFile(stderr)
  except CatchableError:
    discard

proc encodeFrame(payload: string): string =
  result = newStringOfCap(4 + payload.len)
  result.add(char((payload.len shr 24) and 0xFF))
  result.add(char((payload.len shr 16) and 0xFF))
  result.add(char((payload.len shr 8) and 0xFF))
  result.add(char(payload.len and 0xFF))
  result.add(payload)

proc decodeFrameLength(header: string): int =
  if header.len != 4:
    return -1
  (ord(header[0]) shl 24) or
    (ord(header[1]) shl 16) or
    (ord(header[2]) shl 8) or
    ord(header[3])

proc recvExact(client: Socket; size: int; timeoutMs: int): string =
  result = ""
  while result.len < size:
    let chunk = client.recv(size - result.len, timeout = timeoutMs)
    if chunk.len == 0:
      fail("tcp peer closed while reading framed payload")
    result.add(chunk)

proc harnessEnvelope(kind: string; payload: JsonNode): JsonNode =
  %*{
    "version": HarnessTcpProtocolVersion,
    "kind": kind,
    "payload": payload
  }

proc sendJsonOverTcp(
    host: string;
    port: int;
    kind: string;
    payload: JsonNode;
    timeoutMs: int
): bool =
  if host.len == 0 or port <= 0:
    return false
  let frame = encodeFrame($(harnessEnvelope(kind, payload)))
  let deadline = nowMillis() + int64(max(1_000, timeoutMs))
  while nowMillis() <= deadline:
    var client = newSocket()
    try:
      client.connect(host, Port(port))
      client.send(frame)
      return true
    except CatchableError:
      discard
    finally:
      client.close()
    sleep(250)
  false

proc readJsonOverTcpServer(host: string; port: int; waitTimeoutSec: int): JsonNode =
  if port <= 0:
    return newJNull()
  var server = newSocket()
  try:
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(port), host)
    server.listen()
    var readfds = @[server.getFd()]
    if selectRead(readfds, max(1_000, waitTimeoutSec * 1000)) <= 0:
      return newJNull()
    var client: owned(Socket)
    var address = ""
    server.acceptAddr(client, address)
    try:
      let header = recvExact(client, 4, max(1_000, waitTimeoutSec * 1000))
      let payloadLen = decodeFrameLength(header)
      if payloadLen < 0 or payloadLen > 16 * 1024 * 1024:
        return newJNull()
      let payload =
        if payloadLen == 0: ""
        else: recvExact(client, payloadLen, max(1_000, waitTimeoutSec * 1000))
      return parseJsonSafe(payload)
    finally:
      if not client.isNil:
        client.close()
  finally:
    server.close()

proc openRemoteInfoServer(host: string; port: int): RemoteInfoServer =
  if port <= 0:
    return
  result.socket = newSocket()
  result.socket.setSockOpt(OptReuseAddr, true)
  result.socket.bindAddr(Port(port), host)
  result.socket.listen()
  result.active = true

proc closeRemoteInfoServer(server: var RemoteInfoServer) =
  if not server.active:
    return
  server.socket.close()
  server.active = false

proc readJsonOverTcpServer(server: var RemoteInfoServer; waitTimeoutSec: int): JsonNode =
  if not server.active:
    return newJNull()
  var readfds = @[server.socket.getFd()]
  if selectRead(readfds, max(1_000, waitTimeoutSec * 1000)) <= 0:
    return newJNull()
  var client: owned(Socket)
  var address = ""
  server.socket.acceptAddr(client, address)
  try:
    let header = recvExact(client, 4, max(1_000, waitTimeoutSec * 1000))
    let payloadLen = decodeFrameLength(header)
    if payloadLen < 0 or payloadLen > 16 * 1024 * 1024:
      return newJNull()
    let payload =
      if payloadLen == 0: ""
      else: recvExact(client, payloadLen, max(1_000, waitTimeoutSec * 1000))
    return parseJsonSafe(payload)
  finally:
    if not client.isNil:
      client.close()

proc openSignalListener(host: string; port: int): SignalListener =
  if port <= 0:
    return
  result.socket = newSocket()
  result.socket.setSockOpt(OptReuseAddr, true)
  result.socket.bindAddr(Port(port), host)
  result.socket.listen()
  result.active = true

proc closeSignalListener(listener: var SignalListener) =
  if not listener.active:
    return
  listener.socket.close()
  listener.active = false

proc closeSignalChannel(channel: var SignalChannel) =
  if not channel.connected:
    return
  channel.socket.close()
  channel.connected = false

proc acceptSignalChannel(listener: var SignalListener; waitTimeoutSec: int): SignalChannel =
  if not listener.active:
    return
  var readfds = @[listener.socket.getFd()]
  if selectRead(readfds, max(1_000, waitTimeoutSec * 1000)) <= 0:
    return
  var client: owned(Socket)
  var address = ""
  listener.socket.acceptAddr(client, address)
  if client.isNil:
    return
  result.socket = move(client)
  result.connected = true

proc connectSignalChannel(host: string; port: int; waitTimeoutSec: int): SignalChannel =
  if host.len == 0 or port <= 0:
    return
  let deadline = nowMillis() + int64(max(1_000, waitTimeoutSec * 1000))
  while nowMillis() <= deadline:
    var client = newSocket()
    try:
      client.connect(host, Port(port))
      result.socket = client
      result.connected = true
      return
    except CatchableError:
      discard
    finally:
      if not result.connected:
        client.close()
    sleep(250)

proc sendSignal(channel: var SignalChannel; kind: string; payload: JsonNode): bool =
  if not channel.connected:
    return false
  try:
    signalTrace("send kind=" & kind)
    channel.socket.send(encodeFrame($(harnessEnvelope(kind, payload))))
    true
  except CatchableError:
    closeSignalChannel(channel)
    false

proc recvSignal(channel: var SignalChannel; waitTimeoutSec: int): JsonNode =
  if not channel.connected:
    return newJNull()
  let timeoutMs = max(1_000, waitTimeoutSec * 1000)
  try:
    var readfds = @[channel.socket.getFd()]
    if selectRead(readfds, timeoutMs) <= 0:
      return newJNull()
    let header = recvExact(channel.socket, 4, timeoutMs)
    let payloadLen = decodeFrameLength(header)
    if payloadLen < 0 or payloadLen > 16 * 1024 * 1024:
      fail("invalid signal frame length")
    let payload =
      if payloadLen == 0: ""
      else: recvExact(channel.socket, payloadLen, timeoutMs)
    let parsed = parseJsonSafe(payload)
    let kind =
      if parsed.kind == JObject and parsed.hasKey("kind") and parsed["kind"].kind == JString:
        parsed["kind"].getStr()
      else:
        "<invalid>"
    signalTrace("recv kind=" & kind)
    parsed
  except CatchableError:
    closeSignalChannel(channel)
    newJNull()

proc recvSignalMs(channel: var SignalChannel; timeoutMs: int): JsonNode =
  if not channel.connected:
    return newJNull()
  let safeTimeoutMs = max(1, timeoutMs)
  try:
    var readfds = @[channel.socket.getFd()]
    if selectRead(readfds, safeTimeoutMs) <= 0:
      return newJNull()
    let header = recvExact(channel.socket, 4, safeTimeoutMs)
    let payloadLen = decodeFrameLength(header)
    if payloadLen < 0 or payloadLen > 16 * 1024 * 1024:
      fail("invalid signal frame length")
    let payload =
      if payloadLen == 0: ""
      else: recvExact(channel.socket, payloadLen, safeTimeoutMs)
    let parsed = parseJsonSafe(payload)
    let kind =
      if parsed.kind == JObject and parsed.hasKey("kind") and parsed["kind"].kind == JString:
        parsed["kind"].getStr()
      else:
        "<invalid>"
    signalTrace("recv kind=" & kind)
    parsed
  except CatchableError:
    closeSignalChannel(channel)
    newJNull()

proc signalStagePayload(name: string; extra = newJNull()): JsonNode =
  if extra.kind == JObject:
    result = extra
  else:
    result = newJObject()
  result["name"] = %name
  result["sentAtMs"] = %nowMillis()

proc strField(node: JsonNode; key: string; defaultValue = ""): string
proc peerListenAddrs(info: JsonNode): seq[string]
proc filterSupportedDialAddrs(addrs: seq[string]; quicOnly: bool): seq[string]
proc setRemoteMeta(resultNode: JsonNode; peerId: string; addrs: seq[string])

proc waitForSignalKind(
    channel: var SignalChannel;
    waitTimeoutSec: int;
    targetKind: string;
    payloadOut: var JsonNode
): bool =
  let deadline = nowMillis() + int64(max(1_000, waitTimeoutSec * 1000))
  while nowMillis() <= deadline:
    let envelope = recvSignal(channel, 1)
    if envelope.kind != JObject:
      continue
    let version =
      if envelope.hasKey("version") and envelope["version"].kind == JInt:
        envelope["version"].getInt()
      else:
        0
    if version != HarnessTcpProtocolVersion:
      continue
    let kind =
      if envelope.hasKey("kind") and envelope["kind"].kind == JString:
        envelope["kind"].getStr()
      else:
        ""
    if kind == "stage":
      continue
    if kind != targetKind:
      continue
    payloadOut =
      if envelope.hasKey("payload"): envelope["payload"] else: newJNull()
    return true
  false

proc jsonStringArray(values: openArray[string]): JsonNode =
  result = newJArray()
  for value in values:
    result.add(%value)

proc strField(node: JsonNode; key: string; defaultValue = ""): string =
  if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
    return node[key].getStr()
  defaultValue

proc intField(node: JsonNode; key: string; defaultValue = 0): int =
  if node.kind != JObject or not node.hasKey(key):
    return defaultValue
  case node[key].kind
  of JInt:
    node[key].getInt()
  of JFloat:
    int(node[key].getFloat())
  of JString:
    try:
      parseInt(node[key].getStr())
    except ValueError:
      defaultValue
  else:
    defaultValue

proc boolField(node: JsonNode; key: string; defaultValue = false): bool =
  if node.kind != JObject or not node.hasKey(key):
    return defaultValue
  case node[key].kind
  of JBool:
    node[key].getBool()
  of JString:
    node[key].getStr().strip().toLowerAscii() in ["1", "true", "yes", "y"]
  else:
    defaultValue

proc stringSeqField(node: JsonNode; key: string): seq[string] =
  if node.kind != JObject or not node.hasKey(key) or node[key].kind != JArray:
    return @[]
  for item in node[key].items:
    if item.kind == JString:
      let value = item.getStr().strip()
      if value.len > 0:
        result.add(value)

proc hasConcreteTsnetListenAddr(node: JsonNode): bool =
  if node.kind != JArray:
    return false
  for item in node.items:
    if item.kind != JString:
      continue
    let value = item.getStr().strip().toLowerAscii()
    if value.len == 0 or not value.endsWith("/tsnet"):
      continue
    if value.contains("/ip4/0.0.0.0/") or value.contains("/ip6/::/"):
      continue
    if value.contains("/udp/0/") or value.contains("/tcp/0/"):
      continue
    return true
  false

proc findPeerRow(rows: JsonNode; peerId: string): JsonNode =
  if rows.kind != JArray or peerId.len == 0:
    return newJNull()
  for item in rows.items:
    if item.kind == JObject and strField(item, "peerId") == peerId:
      return item
  newJNull()

proc defaultDataDir(label: string): string =
  getTempDir() / "nim-libp2p-tsnet-acceptance" / (label & "-" & $nowMillis())

proc defaultStateDir(label: string): string =
  getTempDir() / "nim-libp2p-tsnet-state" / label

proc defaultHostname(label: string): string =
  let trimmed = label.strip().replace("_", "-").replace(" ", "-")
  if trimmed.len > 0: trimmed else: "tsnet-acceptance"

proc waitUntil(deadlineMs: int64; intervalMs: int; condition: proc(): bool): bool

proc coldStartOnly(opts: HarnessOptions): bool =
  opts.role == hrResponder and
    opts.remoteInfoPath.len == 0 and
    opts.remoteInfoJson.len == 0 and
    opts.remoteInfoJsonBase64.len == 0 and
    not opts.remoteInfoStdin and
    opts.remoteInfoTcpPort <= 0 and
    opts.signalPort <= 0 and
    not opts.emitLocalInfoStdout and
    opts.emitLocalInfoTcpPort <= 0

proc parseRole(raw: string): HarnessRole =
  case raw.strip().toLowerAscii()
  of "", "initiator", "client", "dialer":
    hrInitiator
  of "responder", "server", "listener":
    hrResponder
  else:
    fail("unknown role: " & raw)

proc parseBoolArg(raw: string): bool =
  raw.strip().toLowerAscii() in ["1", "true", "yes", "y", "on"]

proc parseFlagArg(raw: string): bool =
  if raw.strip().len == 0:
    true
  else:
    parseBoolArg(raw)

proc initOptions(): HarnessOptions =
  result = HarnessOptions(
    role: hrInitiator,
    label: "tsnet-acceptance",
    waitTimeoutSec: 120,
    connectTimeoutMs: 30_000,
    wireguardPort: 41641,
    listenPort: 0,
    controlProtocol: "auto",
    logLevel: "debug",
    enableDebug: true,
    dmText: "acceptance-ping",
    pingSamples: 3,
    remoteInfoTcpHost: "127.0.0.1",
    emitLocalInfoTcpHost: "127.0.0.1",
    emitResultTcpHost: "127.0.0.1",
    signalHost: "127.0.0.1",
    quicOnly: false
  )

proc parseOptions(): HarnessOptions =
  result = initOptions()
  let args = commandLineParams()
  var idx = 0
  while idx < args.len:
    let raw = args[idx]
    if not raw.startsWith("-"):
      inc idx
      continue
    var key = raw
    while key.startsWith("-"):
      key = key[1 .. ^1]
    var val = ""
    let eqPos = key.find('=')
    if eqPos >= 0:
      val = key[eqPos + 1 .. ^1]
      key = key[0 ..< eqPos]
    elif idx + 1 < args.len and not args[idx + 1].startsWith("-"):
      val = args[idx + 1]
      inc idx
    case key.normalize()
    of "role":
      result.role = parseRole(val)
    of "label":
      result.label = val.strip()
    of "controlurl":
      result.controlUrl = val.strip()
    of "controlprotocol":
      result.controlProtocol = val.strip().toLowerAscii()
    of "controlendpoint":
      result.controlEndpoint = val.strip()
    of "relayendpoint":
      result.relayEndpoint = val.strip()
    of "authkey":
      result.authKey = val.strip()
    of "hostname":
      result.hostname = val.strip()
    of "statedir":
      result.stateDir = val.strip()
    of "datadir":
      result.dataDir = val.strip()
    of "bridgelib", "bridgelibrarypath":
      result.bridgeLibraryPath = val.strip()
    of "identity", "identitypath":
      result.identityPath = val.strip()
    of "localinfo":
      result.localInfoPath = val.strip()
    of "remoteinfo":
      result.remoteInfoPath = val.strip()
    of "result":
      result.resultPath = val.strip()
    of "waittimeoutsec":
      result.waitTimeoutSec = max(30, parseInt(val))
    of "connecttimeoutms":
      result.connectTimeoutMs = max(5_000, parseInt(val))
    of "wireguardport":
      result.wireguardPort = max(0, parseInt(val))
    of "listenport":
      result.listenPort = max(0, parseInt(val))
    of "loglevel":
      result.logLevel = val.strip()
    of "enabledebug":
      result.enableDebug = parseFlagArg(val)
    of "dmtext":
      result.dmText = val
    of "pingsamples":
      result.pingSamples = max(1, parseInt(val))
    of "nofileoutput":
      result.noFileOutput = parseFlagArg(val)
    of "emitlocalinfostdout":
      result.emitLocalInfoStdout = parseFlagArg(val)
    of "emitresultstdout":
      result.emitResultStdout = parseFlagArg(val)
    of "emitlocalinfotcphost":
      result.emitLocalInfoTcpHost = val.strip()
    of "emitlocalinfotcpport":
      result.emitLocalInfoTcpPort = max(0, parseInt(val))
    of "emitresulttcphost":
      result.emitResultTcpHost = val.strip()
    of "emitresulttcpport":
      result.emitResultTcpPort = max(0, parseInt(val))
    of "signalhost":
      result.signalHost = val.strip()
    of "signalport":
      result.signalPort = max(0, parseInt(val))
    of "remoteinfojson":
      result.remoteInfoJson = val
    of "remoteinfojsonbase64":
      result.remoteInfoJsonBase64 = val.strip()
    of "remoteinfostdin":
      result.remoteInfoStdin = parseFlagArg(val)
    of "remoteinfotcphost":
      result.remoteInfoTcpHost = val.strip()
    of "remoteinfotcpport":
      result.remoteInfoTcpPort = max(0, parseInt(val))
    of "quiconly":
      result.quicOnly = parseFlagArg(val)
    else:
      fail("unknown option: --" & key)
    inc idx
  let networkOrchestrated =
    result.signalPort > 0 or
    result.emitLocalInfoStdout or
    result.emitResultStdout or
    result.emitLocalInfoTcpPort > 0 or
    result.emitResultTcpPort > 0 or
    result.remoteInfoStdin or
    result.remoteInfoTcpPort > 0 or
    result.remoteInfoJson.len > 0 or
    result.remoteInfoJsonBase64.len > 0
  if networkOrchestrated:
    result.noFileOutput = true
  if result.controlUrl.len == 0:
    let protocol = result.controlProtocol.toLowerAscii()
    if result.controlEndpoint.len > 0 and protocol in ["auto", "nim_quic", "nim_tcp", "nim_h3"]:
      result.controlUrl = result.controlEndpoint
    else:
      fail("missing --controlUrl")
  if result.hostname.len == 0:
    result.hostname = defaultHostname(result.label)
  if result.stateDir.len == 0:
    result.stateDir = defaultStateDir(result.label)
  if result.dataDir.len == 0:
    result.dataDir = defaultDataDir(result.label)
  if result.identityPath.len == 0:
    result.identityPath = result.stateDir / "libp2p-identity.json"
  if result.noFileOutput:
    result.localInfoPath = ""
    result.resultPath = ""
  elif result.localInfoPath.len == 0:
    result.localInfoPath = result.dataDir / "local-info.json"
  if not result.noFileOutput and result.resultPath.len == 0:
    result.resultPath = result.dataDir / "result.json"

proc generatedIdentity(): JsonNode =
  let rng = newRng()
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
  let privateKey = pair.seckey.getRawBytes().get()
  let publicKey = pair.pubkey.getRawBytes().get()
  let peerId = $PeerId.init(pair.seckey).get()
  %*{
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "tsnet_acceptance_harness"
  }

proc validIdentity(node: JsonNode): bool =
  node.kind == JObject and
    strField(node, "peerId").len > 0 and
    strField(node, "privateKey").len > 0 and
    strField(node, "publicKey").len > 0

proc stableIdentity(identityPath: string): JsonNode =
  if identityPath.len > 0 and fileExists(identityPath):
    let loaded = parseJsonSafe(readFile(identityPath), "{}")
    if validIdentity(loaded):
      return loaded
  result = generatedIdentity()
  if identityPath.len > 0:
    writeJsonFile(identityPath, result)

proc defaultListenAddrs(opts: HarnessOptions): JsonNode =
  if opts.quicOnly and opts.role == hrInitiator:
    return newJArray()
  let tcpPort = if opts.listenPort > 0: opts.listenPort else: 0
  result = newJArray()
  if not opts.quicOnly:
    result.add(%("/ip4/0.0.0.0/tcp/" & $tcpPort & "/tsnet"))
    result.add(%("/ip6/::/tcp/" & $tcpPort & "/tsnet"))
  when defined(libp2p_msquic_experimental):
    let udpPort = if opts.listenPort > 0: opts.listenPort else: 0
    result.add(%("/ip4/0.0.0.0/udp/" & $udpPort & "/quic-v1/tsnet"))
    result.add(%("/ip6/::/udp/" & $udpPort & "/quic-v1/tsnet"))
  elif opts.quicOnly:
    fail("quicOnly requires libp2p_msquic_experimental")

proc buildConfig(opts: HarnessOptions): JsonNode =
  let disableDefaultTsnetListen = opts.quicOnly and opts.role == hrInitiator
  %*{
    "identity": stableIdentity(opts.identityPath),
    "dataDir": opts.dataDir,
    "listenAddresses": defaultListenAddrs(opts),
    "transportPolicy": (if opts.quicOnly: "quic_only" else: "quic_preferred"),
    "automations": {
      "gossipsub": false,
      "autonat": false,
      "circuitRelay": false
    },
    "extra": {
      "underlay": "tsnet",
      "quicRuntimePreference": "builtin_only",
      "disableMdns": true,
      "disable_mdns": true,
      "disableWanBootstrap": true,
      "disable_wan_bootstrap": true,
      "enable_public_bootstrap": false,
      "disable_public_bootstrap": true,
      "disableNodeTelemetryPubsub": true,
      "disableDefaultTsnetListen": disableDefaultTsnetListen,
      "transportPolicy": (if opts.quicOnly: "quic_only" else: "quic_preferred"),
      "tsnet": {
        "controlUrl": opts.controlUrl,
        "controlProtocol": opts.controlProtocol,
        "controlEndpoint": opts.controlEndpoint,
        "relayEndpoint": opts.relayEndpoint,
        "authKey": opts.authKey,
        "hostname": opts.hostname,
        "stateDir": opts.stateDir,
        "wireguardPort": opts.wireguardPort,
        "bridgeLibraryPath": opts.bridgeLibraryPath,
        "logLevel": opts.logLevel,
        "enableDebug": opts.enableDebug,
        "disableDefaultListen": disableDefaultTsnetListen
      }
    }
  }

proc waitUntil(deadlineMs: int64; intervalMs: int; condition: proc(): bool): bool =
  while nowMillis() <= deadlineMs:
    if condition():
      return true
    sleep(intervalMs)
  false

proc parsePeerInfo(path: string): JsonNode =
  if path.len == 0 or not fileExists(path):
    return newJNull()
  parseJsonSafe(readFile(path))

proc peerListenAddrs(info: JsonNode): seq[string] =
  if info.kind != JObject:
    return @[]
  if info.hasKey("listenAddrs") and info["listenAddrs"].kind == JArray:
    for item in info["listenAddrs"].items:
      if item.kind == JString and item.getStr().strip().len > 0:
        result.add(item.getStr().strip())

proc filterSupportedDialAddrs(addrs: seq[string]; quicOnly: bool): seq[string] =
  when defined(libp2p_msquic_experimental):
    for addr in addrs:
      let lower = addr.toLowerAscii()
      if quicOnly:
        if "/quic-v1/" in lower or lower.endsWith("/quic-v1/tsnet"):
          result.add(addr)
      else:
        result.add(addr)
  else:
    for addr in addrs:
      if "/quic-v1/" notin addr.toLowerAscii():
        result.add(addr)

proc firstTailnetIp(info: JsonNode): string =
  if info.kind != JObject:
    return ""
  let status =
    if info.hasKey("tailnetStatus"): info["tailnetStatus"] else: newJNull()
  if status.kind != JObject:
    return ""
  for item in stringSeqField(status, "tailnetIPs"):
    if item.len > 0:
      return item
  ""

proc roleName(role: HarnessRole): string =
  if role == hrInitiator: "initiator" else: "responder"

proc remoteInfoDeadline(path: string; waitTimeoutSec: int): JsonNode =
  if path.len == 0:
    return newJNull()
  var remote = parsePeerInfo(path)
  let ok = waitUntil(nowMillis() + int64(waitTimeoutSec * 1000), 500, proc(): bool =
    remote = parsePeerInfo(path)
    remote.kind == JObject and strField(remote, "peerId").len > 0 and peerListenAddrs(remote).len > 0
  )
  if ok: remote else: newJNull()

proc remoteInfoFromInline(opts: HarnessOptions): JsonNode =
  if opts.remoteInfoJsonBase64.len > 0:
    try:
      return parseJsonSafe(base64.decode(opts.remoteInfoJsonBase64))
    except CatchableError:
      return newJNull()
  if opts.remoteInfoJson.len > 0:
    return parseJsonSafe(opts.remoteInfoJson)
  newJNull()

proc remoteInfoFromStdin(opts: HarnessOptions): JsonNode =
  if not opts.remoteInfoStdin:
    return newJNull()
  var line = ""
  while stdin.readLine(line):
    let trimmed = line.strip()
    if trimmed.len == 0:
      continue
    if trimmed.startsWith(RemoteInfoStdinPrefix):
      try:
        return parseJsonSafe(base64.decode(trimmed[RemoteInfoStdinPrefix.len .. ^1]))
      except CatchableError:
        return newJNull()
    return parseJsonSafe(trimmed)
  newJNull()

proc remoteInfoFromTcp(opts: HarnessOptions; preboundServer: var RemoteInfoServer): JsonNode =
  if opts.remoteInfoTcpPort <= 0 and not preboundServer.active:
    return newJNull()
  let envelope =
    try:
      if preboundServer.active:
        readJsonOverTcpServer(preboundServer, opts.waitTimeoutSec)
      else:
        readJsonOverTcpServer(opts.remoteInfoTcpHost, opts.remoteInfoTcpPort, opts.waitTimeoutSec)
    except CatchableError:
      newJNull()
  if envelope.kind != JObject:
    return newJNull()
  if intField(envelope, "version", 0) != HarnessTcpProtocolVersion:
    return newJNull()
  if strField(envelope, "kind") != "local_info":
    return newJNull()
  if envelope.hasKey("payload"):
    return envelope["payload"]
  newJNull()

proc resolveRemoteInfo(opts: HarnessOptions; preboundServer: var RemoteInfoServer): JsonNode =
  let inlineInfo = remoteInfoFromInline(opts)
  if inlineInfo.kind == JObject and strField(inlineInfo, "peerId").len > 0:
    return inlineInfo
  let tcpInfo = remoteInfoFromTcp(opts, preboundServer)
  if tcpInfo.kind == JObject and strField(tcpInfo, "peerId").len > 0:
    return tcpInfo
  let stdinInfo = remoteInfoFromStdin(opts)
  if stdinInfo.kind == JObject and strField(stdinInfo, "peerId").len > 0:
    return stdinInfo
  let fileInfo = remoteInfoDeadline(opts.remoteInfoPath, opts.waitTimeoutSec)
  if fileInfo.kind == JObject and strField(fileInfo, "peerId").len > 0:
    return fileInfo
  newJNull()

proc tryRefreshRemoteInfo(opts: HarnessOptions): JsonNode =
  let fileInfo = parsePeerInfo(opts.remoteInfoPath)
  if fileInfo.kind == JObject and strField(fileInfo, "peerId").len > 0:
    return fileInfo
  newJNull()

proc connectedPeerInfo(handle: pointer; peerId: string): JsonNode =
  findPeerRow(safeJsonExpr(libp2p_connected_peers_info(handle), "[]"), peerId)

proc connectedPeerIds(handle: pointer): JsonNode =
  safeJsonExpr(libp2p_get_connected_peers_json(handle), "[]")

proc peerConnected(handle: pointer; peerId: string): bool =
  if peerId.len == 0:
    return false
  libp2p_is_peer_connected(handle, peerId.cstring)

proc lastDirectError(handle: pointer): string =
  safeCStringExpr(libp2p_get_last_direct_error(handle))

proc dmRows(handle: pointer): JsonNode =
  safeJsonExpr(social_received_direct_messages(handle, 64), """{"items":[]}""")

proc resultEnvelope(opts: HarnessOptions): JsonNode =
  %*{
    "ok": false,
    "role": roleName(opts.role),
    "label": opts.label,
    "hostname": opts.hostname,
    "startedAtMs": nowMillis(),
    "errors": []
  }

proc appendError(result: JsonNode; message: string) =
  if result.kind != JObject:
    return
  if not result.hasKey("errors") or result["errors"].kind != JArray:
    result["errors"] = newJArray()
  result["errors"].add(%message)

proc ensureArrayField(target: JsonNode; key: string): JsonNode =
  if target.kind != JObject:
    return newJArray()
  if not target.hasKey(key) or target[key].kind != JArray:
    target[key] = newJArray()
  target[key]

proc recordProgress(
    result: JsonNode;
    signalChannel: var SignalChannel;
    stage: string;
    extra = newJNull()
) =
  if result.kind != JObject:
    return
  let row = %*{
    "stage": stage,
    "atMs": nowMillis()
  }
  if extra.kind != JNull:
    row["info"] = extra
  ensureArrayField(result, "progress").add(row)
  result["exchangeStage"] = %stage
  if signalChannel.connected:
    discard sendSignal(signalChannel, "progress", row)

proc consumeSignalProgress(
    signalChannel: var SignalChannel;
    opts: HarnessOptions;
    resultNode: JsonNode;
    remotePeerId: var string;
    remoteListenAddrs: var seq[string];
    pendingReplyTo: var string;
    responderIncomingAtMs: var int64;
    responderReplySent: var bool;
    replySeen: var bool;
    responderReplyConfirmed: var bool
) =
  if not signalChannel.connected:
    return
  while true:
    let envelope = recvSignalMs(signalChannel, 1)
    if envelope.kind != JObject:
      break
    let version =
      if envelope.hasKey("version") and envelope["version"].kind == JInt:
        envelope["version"].getInt()
      else:
        0
    if version != HarnessTcpProtocolVersion:
      continue
    let kind = strField(envelope, "kind")
    let payload =
      if envelope.kind == JObject and envelope.hasKey("payload"):
        envelope["payload"]
      else:
        newJNull()
    case kind
    of "progress":
      let stage = strField(payload, "stage")
      let info =
        if payload.kind == JObject and payload.hasKey("info"):
          payload["info"]
        else:
          newJNull()
      if opts.role == hrResponder and stage == "initiator_dm_sent":
        let replyTo = strField(info, "messageId")
        if pendingReplyTo.len == 0 and replyTo.len > 0:
          pendingReplyTo = replyTo
          if responderIncomingAtMs == 0:
            responderIncomingAtMs = nowMillis()
          recordProgress(
            resultNode,
            signalChannel,
            "responder_dm_confirmed_via_signal",
            %*{"replyTo": replyTo}
          )
      elif opts.role == hrInitiator and stage == "responder_reply_sent":
        responderReplySent = true
      elif opts.role == hrInitiator and stage == "initiator_reply_received":
        replySeen = true
      elif opts.role == hrResponder and stage == "initiator_reply_received":
        responderReplyConfirmed = true
        recordProgress(
          resultNode,
          signalChannel,
          "responder_reply_confirmed",
          %*{
            "messageId": strField(info, "messageId")
          }
        )
    of "local_info":
      if opts.role == hrResponder:
        let peerId = strField(payload, "peerId")
        if peerId.len > 0:
          remotePeerId = peerId
          remoteListenAddrs = filterSupportedDialAddrs(peerListenAddrs(payload), opts.quicOnly)
          setRemoteMeta(resultNode, remotePeerId, remoteListenAddrs)
    else:
      discard

proc setRemoteMeta(resultNode: JsonNode; peerId: string; addrs: seq[string]) =
  if resultNode.kind != JObject:
    return
  if peerId.len > 0:
    resultNode["remotePeerId"] = %peerId
  resultNode["remoteListenAddrs"] = %addrs

proc classifyLibp2pPath(peerSnapshot: JsonNode; connectPayload: JsonNode): string =
  let selectedPath = strField(peerSnapshot, "selectedPath").strip().toLowerAscii()
  if selectedPath in ["direct", "relay"]:
    return selectedPath
  if boolField(peerSnapshot, "directConnected"):
    return "direct"
  if boolField(peerSnapshot, "relayConnected"):
    return "relay"
  if peerSnapshot.kind == JObject and peerSnapshot.hasKey("selectedRelayed"):
    return (if boolField(peerSnapshot, "selectedRelayed"): "relay" else: "direct")
  if connectPayload.kind == JObject and connectPayload.hasKey("attempts") and
      connectPayload["attempts"].kind == JArray:
    for attempt in connectPayload["attempts"].items:
      if attempt.kind != JObject or not boolField(attempt, "ok"):
        continue
      let attemptPath = strField(attempt, "path").strip().toLowerAscii()
      if attemptPath in ["direct", "relay"]:
        return attemptPath
  if boolField(connectPayload, "ok"):
    return "direct"
  "none"

when isMainModule:
  let opts = parseOptions()
  if opts.bridgeLibraryPath.len > 0:
    putEnv("NIM_TSNET_BRIDGE_LIB", opts.bridgeLibraryPath)

  var result = resultEnvelope(opts)
  var handle: pointer = nil
  var remoteInfoServer: RemoteInfoServer
  var signalListener: SignalListener
  var signalChannel: SignalChannel
  var remotePeerId = ""
  var sentMid = ""
  var replySeen = false
  var responderReplyConfirmed = false
  var responderReplySent = false
  var initiatorDmSent = false
  var pendingReplyTo = ""
  var remoteIp = ""
  var connectPayload = newJNull()
  var tailnetPing = newJNull()
  var finalTailnetStatus = newJObject()
  var finalConnectedPeer = newJNull()
  var finalConnectedPeerIds = newJArray()
  var receivedRows = newJArray()
  var initiatorDmError = ""
  var responderDmError = ""
  var exchangeFailure = ""
  var exchangeStartedAtMs = 0'i64
  var connectedAtMs = 0'i64
  var initiatorDmSentAtMs = 0'i64
  var responderIncomingAtMs = 0'i64
  var responderReplySentAtMs = 0'i64
  var initiatorDmAttemptCount = 0
  var responderReplyAttemptCount = 0
  var nextInitiatorDmAttemptAtMs = 0'i64
  var nextResponderReplyAttemptAtMs = 0'i64

  try:
    # Start the inbound remote-info listener before node startup so
    # symmetric no-file orchestration doesn't race on who listens first.
    if opts.remoteInfoTcpPort > 0:
      remoteInfoServer = openRemoteInfoServer(opts.remoteInfoTcpHost, opts.remoteInfoTcpPort)
    if opts.signalPort > 0 and opts.role == hrResponder:
      signalListener = openSignalListener(opts.signalHost, opts.signalPort)
    elif opts.signalPort > 0 and opts.role == hrInitiator:
      signalChannel = connectSignalChannel(opts.signalHost, opts.signalPort, opts.waitTimeoutSec)
      if not signalChannel.connected:
        fail("failed to establish signal connection")

    let config = buildConfig(opts)
    handle = libp2p_node_init(($config).cstring)
    if handle.isNil:
      fail("libp2p_node_init failed")
    discard libp2p_mdns_set_enabled(handle, false)
    if not libp2p_node_start_kickoff(handle):
      fail("libp2p_node_start_kickoff failed")

    var startStatus = newJObject()
    let started = waitUntil(nowMillis() + 60_000, 150, proc(): bool =
      startStatus = safeJsonExpr(libp2p_get_start_status_json(handle))
      if strField(startStatus, "stage") == "failed":
        return true
      libp2p_node_is_started(handle) and
        safeCStringExpr(libp2p_get_local_peer_id(handle)).len > 0 and
        not boolField(startStatus, "inFlight", false)
    )
    result["startStatus"] = startStatus
    if not started:
      fail("node did not reach started state")
    if strField(startStatus, "stage") == "failed":
      fail("node_start_kickoff failed: " & strField(startStatus, "error"))
    if opts.signalPort > 0:
      if opts.role == hrResponder and not signalChannel.connected:
        signalChannel = acceptSignalChannel(signalListener, opts.waitTimeoutSec)
        if not signalChannel.connected:
          fail("failed to accept signal connection")
        signalTrace("accepted signal connection")
      elif opts.role == hrInitiator:
        signalTrace("connected signal channel")
      discard sendSignal(
        signalChannel,
        "stage",
        signalStagePayload("node_started", %*{"role": roleName(opts.role)})
      )

    let localPeerId = safeCStringExpr(libp2p_get_local_peer_id(handle))
    if localPeerId.len == 0:
      fail("local peer id unavailable")

    var listenAddrs = safeJsonExpr(libp2p_get_listen_addresses(handle), "[]")
    var tailnetStatus =
      if coldStartOnly(opts):
        let fetched = safeJsonExpr(libp2p_tailnet_status_json(handle))
        if fetched.kind == JObject and fetched.len > 0:
          fetched
        else:
          %*{
            "ok": false,
            "deferred": true,
            "providerStage": "deferred",
            "reason": "cold_start_only"
          }
      else:
        newJObject()
    var tailnetStartStatus =
      if coldStartOnly(opts):
        %*{
          "started": false,
          "inFlight": false,
          "stage": "deferred",
          "error": ""
        }
      else:
        newJObject()
    var derpMap =
      if coldStartOnly(opts):
        %*{
          "ok": false,
          "deferred": true,
          "reason": "cold_start_only"
        }
      else:
        newJObject()
    if not coldStartOnly(opts):
      if not libp2p_tailnet_start_kickoff(handle):
        fail("tailnet start kickoff failed")
      let tailnetReady = waitUntil(nowMillis() + 60_000, 250, proc(): bool =
        tailnetStartStatus = safeJsonExpr(libp2p_get_tailnet_start_status_json(handle))
        let stage = strField(tailnetStartStatus, "stage")
        if stage == "failed":
          return true
        if stage != "started":
          return false
        tailnetStatus = safeJsonExpr(libp2p_tailnet_status_json(handle))
        let ready =
          boolField(tailnetStatus, "ok") and
          stringSeqField(tailnetStatus, "tailnetIPs").len > 0
        if not ready:
          return false
        if opts.quicOnly and opts.role == hrResponder:
          return boolField(tailnetStatus, "proxyListenersReady", false)
        true
      )
      if not tailnetReady:
        fail("tailnet status never became ready")
      if strField(tailnetStartStatus, "stage") == "failed":
        fail("tailnet start failed: " & strField(tailnetStartStatus, "error"))
      if opts.signalPort > 0:
        discard sendSignal(
          signalChannel,
          "stage",
          signalStagePayload("tailnet_ready", tailnetStartStatus)
        )
      derpMap = safeJsonExpr(libp2p_tailnet_derp_map(handle))
      if opts.quicOnly and opts.role == hrResponder:
        let listenReady = waitUntil(nowMillis() + 10_000, 100, proc(): bool =
          listenAddrs = safeJsonExpr(libp2p_get_listen_addresses(handle), "[]")
          hasConcreteTsnetListenAddr(listenAddrs)
        )
        if not listenReady:
          fail("concrete tsnet listen addrs never became available")
      else:
        listenAddrs = safeJsonExpr(libp2p_get_listen_addresses(handle), "[]")

    let localInfo = %*{
      "role": roleName(opts.role),
      "label": opts.label,
      "peerId": localPeerId,
      "listenAddrs": listenAddrs,
      "tailnetStartStatus": tailnetStartStatus,
      "tailnetStatus": tailnetStatus,
      "derpMap": derpMap,
      "writtenAtMs": nowMillis()
    }
    writeJsonFile(opts.localInfoPath, localInfo)
    if opts.emitLocalInfoStdout:
      emitTaggedJson(LocalInfoStdoutPrefix, localInfo)
    if opts.signalPort > 0:
      if not sendSignal(signalChannel, "local_info", localInfo):
        fail("failed to emit local_info over signal channel")
    elif opts.emitLocalInfoTcpPort > 0:
      if not sendJsonOverTcp(
        opts.emitLocalInfoTcpHost,
        opts.emitLocalInfoTcpPort,
        "local_info",
        localInfo,
        opts.waitTimeoutSec * 1000
      ):
        fail("failed to emit local_info over tcp")
    elif opts.role == hrResponder and opts.remoteInfoTcpPort > 0:
      if not sendJsonOverTcp(
        opts.remoteInfoTcpHost,
        opts.remoteInfoTcpPort,
        "local_info",
        localInfo,
        opts.waitTimeoutSec * 1000
      ):
        fail("failed to emit responder local_info over tcp")

    result["peerId"] = %localPeerId
    result["listenAddrs"] = listenAddrs
    result["tailnetStartStatus"] = tailnetStartStatus
    result["tailnetStatusBefore"] = tailnetStatus
    result["derpMap"] = derpMap
    result["coldStartOnly"] = %coldStartOnly(opts)

    if coldStartOnly(opts):
      result["ok"] = %true
      result["tailnetDeferredReady"] = %false

    var remote = newJNull()
    if opts.signalPort > 0:
      if opts.role == hrInitiator:
        var signalRemote = newJNull()
        if not waitForSignalKind(signalChannel, opts.waitTimeoutSec, "local_info", signalRemote):
          fail("remote local_info unavailable over signal channel")
        remote = signalRemote
      else:
        var connectNow = newJNull()
        if not waitForSignalKind(signalChannel, opts.waitTimeoutSec, "connect_now", connectNow):
          fail("connect_now not received over signal channel")
        if connectNow.kind == JObject:
          let senderPeerId = strField(connectNow, "senderPeerId")
          let announcedPeerId =
            if senderPeerId.len > 0:
              senderPeerId
            else:
              strField(connectNow, "remotePeerId")
          if announcedPeerId.len > 0:
            remotePeerId = announcedPeerId
    else:
      remote = resolveRemoteInfo(opts, remoteInfoServer)
    if opts.role == hrInitiator:
      if remote.kind != JObject:
        fail("remote peer info unavailable: " & opts.remoteInfoPath)
      remotePeerId = strField(remote, "peerId")
      if remotePeerId.len == 0:
        fail("remote peer id missing")
    else:
      if remote.kind == JObject:
        remotePeerId = strField(remote, "peerId")
    var remoteListenAddrs =
      if remote.kind == JObject:
        filterSupportedDialAddrs(peerListenAddrs(remote), opts.quicOnly)
      else:
        @[]
    if opts.role == hrInitiator and remoteListenAddrs.len == 0:
      fail("remote listen addrs missing")

    setRemoteMeta(result, remotePeerId, remoteListenAddrs)

    remoteIp =
      if remote.kind == JObject:
        firstTailnetIp(remote)
      else:
        ""

    if opts.role == hrInitiator:
      if opts.signalPort > 0:
        discard sendSignal(
          signalChannel,
          "connect_now",
          %*{
            "senderPeerId": localPeerId,
            "targetPeerId": remotePeerId,
            "sentAtMs": nowMillis()
          }
        )
      connectPayload = safeJsonExpr(
        libp2p_bootstrap_seed_connect_exact(
          handle,
          remotePeerId.cstring,
          ($(%remoteListenAddrs)).cstring,
          "tsnet_acceptance".cstring,
          cint(remoteListenAddrs.len),
          cint(opts.connectTimeoutMs)
        )
      )
      result["connectExact"] = connectPayload
      if not boolField(connectPayload, "ok"):
        fail("connect_exact failed")
      recordProgress(
        result,
        signalChannel,
        "connect_exact_ok",
        %*{
          "attemptedAddr": strField(connectPayload, "attemptedAddr"),
          "attemptCount": intField(connectPayload, "attemptCount", 0)
        }
      )
      sleep(750)
      sentMid = "tsnet-accept-" & $nowMillis()
      result["sentMessageId"] = %sentMid
    else:
      result["connectExact"] = newJNull()

    let resultNode = result
    exchangeStartedAtMs = nowMillis()
    finalTailnetStatus = tailnetStatus
    recordProgress(
      result,
      signalChannel,
      "exchange_waiting",
      %*{"role": roleName(opts.role)}
    )
    let exchangeOk =
      if coldStartOnly(opts):
        true
      else:
        waitUntil(nowMillis() + int64(opts.waitTimeoutSec * 1000), 250, proc(): bool =
          consumeSignalProgress(
            signalChannel,
            opts,
            resultNode,
            remotePeerId,
            remoteListenAddrs,
            pendingReplyTo,
            responderIncomingAtMs,
            responderReplySent,
            replySeen,
            responderReplyConfirmed
          )
          if opts.role == hrResponder and remotePeerId.len == 0:
            let refreshedRemote = tryRefreshRemoteInfo(opts)
            if refreshedRemote.kind == JObject:
              remote = refreshedRemote
              remotePeerId = strField(refreshedRemote, "peerId")
              remoteListenAddrs = filterSupportedDialAddrs(peerListenAddrs(refreshedRemote), opts.quicOnly)
              remoteIp = firstTailnetIp(refreshedRemote)
              setRemoteMeta(resultNode, remotePeerId, remoteListenAddrs)
          let currentlyConnected = peerConnected(handle, remotePeerId)
          finalConnectedPeerIds = connectedPeerIds(handle)
          if finalConnectedPeer.kind != JObject and currentlyConnected:
            let connectionNode =
              if opts.role == hrInitiator and connectPayload.kind == JObject and
                  connectPayload.hasKey("connection") and connectPayload["connection"].kind == JObject:
                connectPayload["connection"]
              else:
                newJObject()
            finalConnectedPeer = %*{
              "peerId": remotePeerId,
              "selectedPath":
                strField(connectionNode, "selectedPath", "unknown"),
              "selectedRelayed":
                boolField(connectionNode, "selectedRelayed", false)
            }
          resultNode["exchangeDiagnostics"] = %*{
            "connected": currentlyConnected,
            "connectedPeers": finalConnectedPeerIds,
            "connectedPeer": finalConnectedPeer,
            "tailnetStatus": finalTailnetStatus,
            "bootstrapStatus":
              (if connectPayload.kind == JObject and connectPayload.hasKey("bootstrap"):
                connectPayload["bootstrap"]
               else:
                newJNull()),
            "lastDirectError": lastDirectError(handle),
            "initiatorDmAttemptCount": initiatorDmAttemptCount,
            "responderReplyAttemptCount": responderReplyAttemptCount,
            "pendingReplyTo": pendingReplyTo,
            "replySeen": replySeen,
            "dmSent": initiatorDmSent
          }
          if currentlyConnected and connectedAtMs == 0:
            connectedAtMs = nowMillis()
            recordProgress(
              resultNode,
              signalChannel,
              "peer_connected",
              %*{
                "peerId": remotePeerId,
                "connectedPeers": finalConnectedPeerIds,
                "connectedPeer": finalConnectedPeer
              }
            )
          if opts.role == hrInitiator and not initiatorDmSent and finalConnectedPeer.kind == JObject:
            let nowMs = nowMillis()
            if nowMs >= nextInitiatorDmAttemptAtMs:
              initiatorDmAttemptCount.inc()
              let body = opts.dmText & "|" & opts.label
              recordProgress(
                resultNode,
                signalChannel,
                "initiator_dm_attempt",
                %*{
                  "attempt": initiatorDmAttemptCount,
                  "peerId": remotePeerId,
                  "messageId": sentMid
                }
              )
              initiatorDmSent = libp2p_send_direct_text(
                handle,
                remotePeerId.cstring,
                sentMid.cstring,
                body.cstring,
                nil,
                true,
                cint(max(10_000, opts.connectTimeoutMs))
              )
              if initiatorDmSent:
                initiatorDmSentAtMs = nowMs
                recordProgress(
                  resultNode,
                  signalChannel,
                  "initiator_dm_sent",
                  %*{
                    "attempt": initiatorDmAttemptCount,
                    "messageId": sentMid
                  }
                )
              else:
                initiatorDmError = lastDirectError(handle)
                nextInitiatorDmAttemptAtMs = nowMs + 1_000
          let rows = dmRows(handle)
          if rows.kind == JObject and rows.hasKey("items") and rows["items"].kind == JArray:
            for item in rows["items"].items:
              receivedRows.add(item)
              let fromPeer = strField(item, "peerId", strField(item, "from"))
              let incomingMid = strField(item, "messageId", strField(item, "mid"))
              let incomingBody = strField(item, "body", strField(item, "content"))
              if opts.role == hrResponder and incomingMid.len > 0:
                if remotePeerId.len == 0 and fromPeer.len > 0:
                  remotePeerId = fromPeer
                  setRemoteMeta(resultNode, remotePeerId, remoteListenAddrs)
                if remotePeerId.len == 0 or fromPeer == remotePeerId:
                  pendingReplyTo = incomingMid
                  if responderIncomingAtMs == 0:
                    responderIncomingAtMs = nowMillis()
                    recordProgress(
                      resultNode,
                      signalChannel,
                      "responder_dm_received",
                      %*{
                        "fromPeer": fromPeer,
                        "messageId": incomingMid
                      }
                    )
              elif opts.role == hrInitiator and fromPeer == remotePeerId and incomingBody.contains("acceptance-reply|"):
                replySeen = true
                recordProgress(
                  resultNode,
                  signalChannel,
                  "initiator_reply_received",
                  %*{
                    "fromPeer": fromPeer,
                    "messageId": incomingMid
                  }
                )
          if opts.role == hrResponder and not responderReplySent and pendingReplyTo.len > 0:
            let nowMs = nowMillis()
            if nowMs >= nextResponderReplyAttemptAtMs:
              responderReplyAttemptCount.inc()
              let replyMid = "tsnet-reply-" & $nowMs
              let replyBody = "acceptance-reply|" & pendingReplyTo & "|" & opts.label
              recordProgress(
                resultNode,
                signalChannel,
                "responder_reply_attempt",
                %*{
                  "attempt": responderReplyAttemptCount,
                  "replyTo": pendingReplyTo
                }
              )
              responderReplySent = libp2p_send_direct_text(
                handle,
                remotePeerId.cstring,
                replyMid.cstring,
                replyBody.cstring,
                pendingReplyTo.cstring,
                false,
                cint(max(10_000, opts.connectTimeoutMs))
              )
              if responderReplySent:
                responderReplySentAtMs = nowMs
                recordProgress(
                  resultNode,
                  signalChannel,
                  "responder_reply_sent",
                  %*{
                    "attempt": responderReplyAttemptCount,
                    "replyTo": pendingReplyTo
                  }
                )
              else:
                responderDmError = lastDirectError(handle)
                nextResponderReplyAttemptAtMs = nowMs + 1_000
          let phaseBudgetMs = max(5_000, min(max(10_000, opts.connectTimeoutMs), opts.waitTimeoutSec * 1000 div 2))
          let nowMs = nowMillis()
          if exchangeFailure.len == 0 and connectedAtMs > 0:
            if opts.role == hrInitiator:
              if not initiatorDmSent and initiatorDmAttemptCount >= 1 and initiatorDmError.len > 0 and
                  nowMs - connectedAtMs >= 1_000:
                exchangeFailure = "initiator_dm_send_failed: " & initiatorDmError
                return true
              if initiatorDmSent and not replySeen and initiatorDmSentAtMs > 0 and
                  nowMs - initiatorDmSentAtMs >= phaseBudgetMs:
                exchangeFailure = "initiator_reply_not_received_after_dm_send"
                return true
            else:
              if pendingReplyTo.len == 0 and nowMs - connectedAtMs >= phaseBudgetMs:
                exchangeFailure = "responder_dm_not_received_after_peer_connected"
                return true
              if pendingReplyTo.len > 0 and not responderReplySent and responderReplyAttemptCount >= 1 and
                  responderDmError.len > 0 and responderIncomingAtMs > 0 and
                  nowMs - responderIncomingAtMs >= 1_000:
                exchangeFailure = "responder_reply_send_failed: " & responderDmError
                return true
              if responderReplySent and not responderReplyConfirmed and
                  responderReplySentAtMs > 0 and
                  nowMs - responderReplySentAtMs >= phaseBudgetMs:
                exchangeFailure = "responder_reply_not_confirmed_by_peer"
                return true
          if opts.role == hrResponder:
            responderReplySent and pendingReplyTo.len > 0 and
              (not signalChannel.connected or responderReplyConfirmed)
          else:
            initiatorDmSent and replySeen
        )
    result["receivedDm"] = receivedRows
    result["connectedPeers"] = finalConnectedPeerIds
    result["connectedPeer"] = finalConnectedPeer
    result["tailnetStatusAfter"] = finalTailnetStatus
    result["replySeen"] = %replySeen
    result["responderReplyConfirmed"] = %responderReplyConfirmed
    result["responderReplySent"] = %responderReplySent
    result["replySent"] = %responderReplySent
    result["dmSent"] = %initiatorDmSent
    result["initiatorDmError"] = %initiatorDmError
    result["responderDmError"] = %responderDmError
    result["replyError"] = %responderDmError
    result["pendingReplyTo"] = %pendingReplyTo
    result["libp2pPath"] = %classifyLibp2pPath(finalConnectedPeer, connectPayload)

    let tailnetPath = strField(finalTailnetStatus, "tailnetPath")
    let tailnetRelay = strField(finalTailnetStatus, "tailnetRelay")
    result["tailnetPathObserved"] =
      %(
        if tailnetPath in ["relay", "punched_direct", "direct"]:
          tailnetPath
        elif tailnetPath == "derp":
          "relay"
        else:
          "unknown"
      )
    result["tailnetRelayObserved"] = %tailnetRelay
    if remoteIp.len > 0:
      let pingReq = %*{
        "peerIP": remoteIp,
        "sampleCount": opts.pingSamples,
        "timeoutMs": 4_000
      }
      tailnetPing = safeJsonExpr(libp2p_tailnet_ping(handle, ($pingReq).cstring))
      result["tailnetPing"] = tailnetPing
    if exchangeFailure.len > 0:
      fail(exchangeFailure)
    if not exchangeOk:
      fail("timed out waiting for peer connection or DM exchange")
    result["ok"] = %true
  except CatchableError as exc:
    appendError(result, exc.msg)
  finally:
    closeRemoteInfoServer(remoteInfoServer)
    if opts.signalPort > 0:
      discard sendSignal(signalChannel, "result", result)
    closeSignalChannel(signalChannel)
    closeSignalListener(signalListener)
    result["finishedAtMs"] = %nowMillis()
    writeJsonFile(opts.resultPath, result)
    if opts.emitResultStdout:
      emitTaggedJson(ResultStdoutPrefix, result)
    if opts.emitResultTcpPort > 0:
      discard sendJsonOverTcp(
        opts.emitResultTcpHost,
        opts.emitResultTcpPort,
        "result",
        result,
        opts.waitTimeoutSec * 1000
      )
    if handle != nil:
      discard libp2p_node_stop(handle)
      sleep(250)
      libp2p_node_free(handle)
    if not boolField(result, "ok"):
      quit(1)
