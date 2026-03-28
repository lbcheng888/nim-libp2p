{.push raises: [].}

import std/[json, os, parseopt, strutils]

import ../libp2p/transports/[tsnet/runtime, tsnet/control, tsnet/state, tsnetprovidertypes]
when defined(libp2p_msquic_experimental):
  import "../libp2p/transports/nim-msquic/api/diagnostics_model"
import ../libp2p/utility

type ProbeArgs = object
  controlUrl: string
  controlEndpoint: string
  relayEndpoint: string
  authKey: string
  hostname: string
  stateDir: string
  wireguardPort: int
  controlProtocol: string
  enableDebug: bool
  headscaleJson: string

proc parseBool(text: string): bool =
  text.strip().toLowerAscii() in ["1", "true", "yes", "y", "on"]

proc jsonString(node: JsonNode, keys: openArray[string]): string =
  if node.isNil or node.kind != JObject:
    return ""
  for key in keys:
    if node.hasKey(key):
      let value = node.getOrDefault(key)
      if value.kind == JString:
        return value.getStr().strip()
  ""

proc jsonInt(node: JsonNode, keys: openArray[string], defaultValue = 0): int =
  if node.isNil or node.kind != JObject:
    return defaultValue
  for key in keys:
    if node.hasKey(key):
      let value = node.getOrDefault(key)
      case value.kind
      of JInt:
        return value.getBiggestInt().int
      of JFloat:
        return value.getFloat().int
      else:
        discard
  defaultValue

proc applyHeadscaleJson(args: var ProbeArgs): Result[void, string] =
  let path = args.headscaleJson.strip()
  if path.len == 0:
    return ok()
  if not fileExists(path):
    return err("missing headscale json: " & path)
  let payload =
    try:
      parseFile(path)
    except CatchableError as exc:
      return err("failed to parse headscale json " & path & ": " & exc.msg)
  args.controlUrl = jsonString(payload, ["controlUrl", "controlURL"]).strip()
  args.authKey = jsonString(payload, ["authKey"]).strip()
  if args.hostname.len == 0:
    args.hostname = "nim-tsnet-probe"
  if args.stateDir.len == 0:
    args.stateDir = getTempDir() / "nim-libp2p-tsnet-probe"
  if args.wireguardPort <= 0:
    args.wireguardPort = 41641
  ok()

proc parseArgs(): Result[ProbeArgs, string] =
  var args = ProbeArgs(
    wireguardPort: 41641,
    hostname: "nim-tsnet-probe",
    stateDir: getTempDir() / "nim-libp2p-tsnet-probe",
    controlProtocol: TsnetControlProtocolAuto,
    enableDebug: false
  )
  var parser = initOptParser(commandLineParams())
  while true:
    parser.next()
    case parser.kind
    of cmdEnd:
      break
    of cmdLongOption, cmdShortOption:
      case parser.key
      of "headscale-json":
        args.headscaleJson = parser.val
      of "controlUrl", "control-url":
        args.controlUrl = parser.val
      of "controlEndpoint", "control-endpoint":
        args.controlEndpoint = parser.val
      of "relayEndpoint", "relay-endpoint":
        args.relayEndpoint = parser.val
      of "authKey", "auth-key":
        args.authKey = parser.val
      of "hostname":
        args.hostname = parser.val
      of "stateDir", "state-dir":
        args.stateDir = parser.val
      of "wireguardPort", "wireguard-port":
        try:
          args.wireguardPort = parseInt(parser.val)
        except ValueError:
          return err("invalid wireguard port: " & parser.val)
      of "controlProtocol", "control-protocol":
        args.controlProtocol = normalizeControlProtocol(parser.val)
      of "enableDebug", "enable-debug":
        args.enableDebug =
          if parser.val.len == 0: true
          else: parseBool(parser.val)
      else:
        return err("unknown option: --" & parser.key)
    of cmdArgument:
      return err("unexpected argument: " & parser.key)
  let loaded = args.applyHeadscaleJson()
  if loaded.isErr():
    return err(loaded.error)
  if args.controlUrl.strip().len == 0:
    return err("missing --controlUrl or --headscale-json")
  if args.hostname.strip().len == 0:
    return err("missing hostname")
  if args.stateDir.strip().len == 0:
    return err("missing stateDir")
  ok(args)

proc main(): int =
  let parsed = parseArgs().valueOr:
    echo error
    return 2

  when defined(libp2p_msquic_experimental):
    if parsed.enableDebug or getEnv("NIM_TSNET_PROBE_DIAG").strip() in ["1", "true", "TRUE", "yes", "YES"]:
      clearDiagnosticsHooks()
      registerDiagnosticsHook(proc(event: DiagnosticsEvent) {.gcsafe.} =
        try:
          stderr.writeLine("[probe-diag] " & event.note)
        except IOError:
          discard
      )
      defer:
        clearDiagnosticsHooks()

  let cfg = TsnetProviderConfig(
    controlUrl: parsed.controlUrl,
    controlEndpoint: parsed.controlEndpoint,
    relayEndpoint: parsed.relayEndpoint,
    authKey: parsed.authKey,
    hostname: parsed.hostname,
    stateDir: parsed.stateDir,
    wireguardPort: parsed.wireguardPort,
    controlProtocol: parsed.controlProtocol,
    bridgeLibraryPath: "",
    logLevel: "",
    enableDebug: parsed.enableDebug,
    bridgeExtraJson: ""
  )
  let runtime = TsnetInAppRuntime.new(cfg)
  let started = runtime.start()

  var payload = newJObject()
  payload["ok"] = %started.isOk()
  if started.isErr():
    payload["startError"] = %started.error
  payload["sslCompiled"] = %defined(ssl)
  payload["runtimeStatus"] = %($runtime.status)
  payload["runtimeReady"] = %runtime.ready()
  payload["controlProtocolRequested"] = %runtime.controlProtocolRequested
  payload["controlProtocolSelected"] = %runtime.controlProtocolSelected
  payload["controlEndpointSelected"] = %runtime.controlEndpoint
  payload["controlFallback"] = %runtime.controlFallback
  payload["runtimeCapabilities"] = runtime.capabilities().toJson()
  payload["statePath"] = %runtime.statePath()
  payload["oraclePath"] = %runtime.oraclePath()
  payload["state"] = runtime.state.toJson()
  payload["control"] = runtime.control.toJson()
  let statusPayload = runtime.statusPayload()
  payload["tailnetStatus"] =
    if statusPayload.isOk(): statusPayload.get()
    else: %*{"ok": false, "error": statusPayload.error}
  let pingPayload = runtime.pingPayload(newJObject())
  payload["tailnetPing"] =
    if pingPayload.isOk(): pingPayload.get()
    else: %*{"ok": false, "error": pingPayload.error}
  let derpMapPayload = runtime.derpMapPayload()
  payload["tailnetDerpMap"] =
    if derpMapPayload.isOk(): derpMapPayload.get()
    else: %*{"ok": false, "error": derpMapPayload.error}
  echo $payload
  if started.isOk():
    return 0
  if runtime.control.available():
    return 0
  1

quit(main())
