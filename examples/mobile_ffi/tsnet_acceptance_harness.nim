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

  HarnessOptions = object
    role: HarnessRole
    label: string
    controlUrl: string
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
    remoteInfoJson: string
    remoteInfoJsonBase64: string
    remoteInfoStdin: bool
    remoteInfoTcpHost: string
    remoteInfoTcpPort: int

const
  LocalInfoStdoutPrefix = "TSNET_LOCAL_INFO_JSON_B64="
  ResultStdoutPrefix = "TSNET_RESULT_JSON_B64="
  RemoteInfoStdinPrefix = "TSNET_REMOTE_INFO_JSON_B64="

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
    logLevel: "debug",
    enableDebug: true,
    dmText: "acceptance-ping",
    pingSamples: 3,
    remoteInfoTcpHost: "127.0.0.1"
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
    else:
      fail("unknown option: --" & key)
    inc idx
  if result.controlUrl.len == 0:
    fail("missing --controlUrl")
  if result.authKey.len == 0:
    fail("missing --authKey")
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

proc defaultListenAddrs(listenPort: int): JsonNode =
  let tcpPort = if listenPort > 0: listenPort else: 0
  result = %*[
    "/ip4/0.0.0.0/tcp/" & $tcpPort & "/tsnet",
    "/ip6/::/tcp/" & $tcpPort & "/tsnet"
  ]
  when defined(libp2p_msquic_experimental):
    let udpPort = if listenPort > 0: listenPort else: 0
    result.add(%("/ip4/0.0.0.0/udp/" & $udpPort & "/quic-v1/tsnet"))
    result.add(%("/ip6/::/udp/" & $udpPort & "/quic-v1/tsnet"))

proc buildConfig(opts: HarnessOptions): JsonNode =
  %*{
    "identity": stableIdentity(opts.identityPath),
    "dataDir": opts.dataDir,
    "listenAddresses": defaultListenAddrs(opts.listenPort),
    "automations": {
      "gossipsub": false,
      "autonat": false,
      "circuitRelay": false
    },
    "extra": {
      "underlay": "tsnet",
      "disableMdns": true,
      "disable_mdns": true,
      "disableWanBootstrap": true,
      "disable_wan_bootstrap": true,
      "enable_public_bootstrap": false,
      "disable_public_bootstrap": true,
      "disableNodeTelemetryPubsub": true,
      "tsnet": {
        "controlUrl": opts.controlUrl,
        "authKey": opts.authKey,
        "hostname": opts.hostname,
        "stateDir": opts.stateDir,
        "wireguardPort": opts.wireguardPort,
        "bridgeLibraryPath": opts.bridgeLibraryPath,
        "logLevel": opts.logLevel,
        "enableDebug": opts.enableDebug
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

proc filterSupportedDialAddrs(addrs: seq[string]): seq[string] =
  when defined(libp2p_msquic_experimental):
    result = addrs
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

proc remoteInfoFromTcp(opts: HarnessOptions): JsonNode =
  if opts.remoteInfoTcpPort <= 0:
    return newJNull()
  var server = newSocket()
  try:
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(opts.remoteInfoTcpPort), opts.remoteInfoTcpHost)
    server.listen()
    var readfds = @[server.getFd()]
    if selectRead(readfds, max(1_000, opts.waitTimeoutSec * 1000)) <= 0:
      return newJNull()
    var client: owned(Socket)
    var address = ""
    server.acceptAddr(client, address)
    try:
      let payload = client.recvLine(timeout = max(1_000, opts.waitTimeoutSec * 1000))
      let trimmed = payload.strip()
      if trimmed.len == 0:
        return newJNull()
      if trimmed.startsWith(RemoteInfoStdinPrefix):
        try:
          return parseJsonSafe(base64.decode(trimmed[RemoteInfoStdinPrefix.len .. ^1]))
        except CatchableError:
          return newJNull()
      return parseJsonSafe(trimmed)
    finally:
      if not client.isNil:
        client.close()
  finally:
    server.close()

proc resolveRemoteInfo(opts: HarnessOptions): JsonNode =
  let inlineInfo = remoteInfoFromInline(opts)
  if inlineInfo.kind == JObject and strField(inlineInfo, "peerId").len > 0:
    return inlineInfo
  let fileInfo = remoteInfoDeadline(opts.remoteInfoPath, opts.waitTimeoutSec)
  if fileInfo.kind == JObject and strField(fileInfo, "peerId").len > 0:
    return fileInfo
  let tcpInfo = remoteInfoFromTcp(opts)
  if tcpInfo.kind == JObject and strField(tcpInfo, "peerId").len > 0:
    return tcpInfo
  let stdinInfo = remoteInfoFromStdin(opts)
  if stdinInfo.kind == JObject and strField(stdinInfo, "peerId").len > 0:
    return stdinInfo
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
  var remotePeerId = ""
  var sentMid = ""
  var replySeen = false
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

  try:
    let config = buildConfig(opts)
    handle = libp2p_node_init(($config).cstring)
    if handle.isNil:
      fail("libp2p_node_init failed")
    discard libp2p_mdns_set_enabled(handle, false)
    if libp2p_node_start(handle) != 0:
      fail("libp2p_node_start failed")

    let started = waitUntil(nowMillis() + 20_000, 150, proc(): bool =
      libp2p_node_is_started(handle) and safeCStringExpr(libp2p_get_local_peer_id(handle)).len > 0
    )
    if not started:
      fail("node did not reach started state")

    let localPeerId = safeCStringExpr(libp2p_get_local_peer_id(handle))
    if localPeerId.len == 0:
      fail("local peer id unavailable")

    var tailnetStatus = newJObject()
    let tailnetReady = waitUntil(nowMillis() + 60_000, 500, proc(): bool =
      tailnetStatus = safeJsonExpr(libp2p_tailnet_status_json(handle))
      boolField(tailnetStatus, "ok") and stringSeqField(tailnetStatus, "tailnetIPs").len > 0
    )
    if not tailnetReady:
      fail("tailnet status never became ready")
    let derpMap = safeJsonExpr(libp2p_tailnet_derp_map(handle))
    let listenAddrs = safeJsonExpr(libp2p_get_listen_addresses(handle), "[]")

    let localInfo = %*{
      "role": roleName(opts.role),
      "label": opts.label,
      "peerId": localPeerId,
      "listenAddrs": listenAddrs,
      "tailnetStatus": tailnetStatus,
      "derpMap": derpMap,
      "writtenAtMs": nowMillis()
    }
    writeJsonFile(opts.localInfoPath, localInfo)
    if opts.emitLocalInfoStdout:
      emitTaggedJson(LocalInfoStdoutPrefix, localInfo)

    result["peerId"] = %localPeerId
    result["listenAddrs"] = listenAddrs
    result["tailnetStatusBefore"] = tailnetStatus
    result["derpMap"] = derpMap

    let remote = resolveRemoteInfo(opts)
    if remote.kind != JObject:
      fail("remote peer info unavailable: " & opts.remoteInfoPath)
    remotePeerId = strField(remote, "peerId")
    if remotePeerId.len == 0:
      fail("remote peer id missing")
    let remoteListenAddrs = filterSupportedDialAddrs(peerListenAddrs(remote))
    if remoteListenAddrs.len == 0:
      fail("remote listen addrs missing")

    result["remotePeerId"] = %remotePeerId
    result["remoteListenAddrs"] = %remoteListenAddrs

    remoteIp = firstTailnetIp(remote)

    if opts.role == hrInitiator:
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
      sleep(750)
      sentMid = "tsnet-accept-" & $nowMillis()
      result["sentMessageId"] = %sentMid
    else:
      result["connectExact"] = newJNull()

    let exchangeOk = waitUntil(nowMillis() + int64(opts.waitTimeoutSec * 1000), 250, proc(): bool =
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
      finalTailnetStatus = safeJsonExpr(libp2p_tailnet_status_json(handle))
      if opts.role == hrInitiator and not initiatorDmSent and finalConnectedPeer.kind == JObject:
        let body = opts.dmText & "|" & opts.label
        initiatorDmSent = libp2p_send_direct_text(
          handle,
          remotePeerId.cstring,
          sentMid.cstring,
          body.cstring,
          nil,
          true,
          cint(max(10_000, opts.connectTimeoutMs))
        )
        if not initiatorDmSent:
          initiatorDmError = lastDirectError(handle)
      let rows = dmRows(handle)
      if rows.kind == JObject and rows.hasKey("items") and rows["items"].kind == JArray:
        for item in rows["items"].items:
          receivedRows.add(item)
          let fromPeer = strField(item, "peerId", strField(item, "from"))
          let incomingMid = strField(item, "messageId", strField(item, "mid"))
          let incomingBody = strField(item, "body", strField(item, "content"))
          if opts.role == hrResponder and fromPeer == remotePeerId and incomingMid.len > 0:
            pendingReplyTo = incomingMid
          elif opts.role == hrInitiator and fromPeer == remotePeerId and incomingBody.contains("acceptance-reply|"):
            replySeen = true
      if opts.role == hrResponder and not responderReplySent and pendingReplyTo.len > 0:
        let replyMid = "tsnet-reply-" & $nowMillis()
        let replyBody = "acceptance-reply|" & pendingReplyTo & "|" & opts.label
        responderReplySent = libp2p_send_direct_text(
          handle,
          remotePeerId.cstring,
          replyMid.cstring,
          replyBody.cstring,
          pendingReplyTo.cstring,
          true,
          cint(max(10_000, opts.connectTimeoutMs))
        )
        if not responderReplySent:
          responderDmError = lastDirectError(handle)
      if opts.role == hrResponder:
        responderReplySent and pendingReplyTo.len > 0
      else:
        initiatorDmSent and replySeen
    )
    result["receivedDm"] = receivedRows
    result["connectedPeers"] = finalConnectedPeerIds
    let connectedInfo = connectedPeerInfo(handle, remotePeerId)
    if connectedInfo.kind == JObject:
      let currentPath = strField(finalConnectedPeer, "selectedPath").strip().toLowerAscii()
      if finalConnectedPeer.kind != JObject or currentPath.len == 0 or
          currentPath in ["unknown", "none"]:
        finalConnectedPeer = connectedInfo
    result["connectedPeer"] = finalConnectedPeer
    result["tailnetStatusAfter"] = finalTailnetStatus
    result["replySeen"] = %replySeen
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
        if tailnetPath == "derp" and tailnetRelay.len > 0:
          "DERP(" & tailnetRelay & ")"
        elif tailnetPath == "direct":
          "DIRECT"
        elif tailnetPath.len > 0:
          tailnetPath
        else:
          "unknown"
      )
    if remoteIp.len > 0:
      let pingReq = %*{
        "peerIP": remoteIp,
        "sampleCount": opts.pingSamples,
        "timeoutMs": 4_000
      }
      tailnetPing = safeJsonExpr(libp2p_tailnet_ping(handle, ($pingReq).cstring))
      result["tailnetPing"] = tailnetPing
    if not exchangeOk:
      fail("timed out waiting for peer connection or DM exchange")
    result["ok"] = %true
  except CatchableError as exc:
    appendError(result, exc.msg)
  finally:
    result["finishedAtMs"] = %nowMillis()
    writeJsonFile(opts.resultPath, result)
    if opts.emitResultStdout:
      emitTaggedJson(ResultStdoutPrefix, result)
    if handle != nil:
      discard libp2p_node_stop(handle)
      sleep(250)
      libp2p_node_free(handle)
    if not boolField(result, "ok"):
      quit(1)
