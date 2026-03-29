import std/[json, os, parseopt, strutils]
import chronos

import ../libp2p/transports/tsnet/[control, quiccontrol, quicrelay]
import "../libp2p/transports/nim-msquic/api/diagnostics_model"
import ../libp2p/utility

type
  GatewayArgs = object
    listenHost: string
    controlListenPort: int
    relayListenPort: int
    controlUrl: string
    certificateFile: string
    privateKeyFile: string
    openNetwork: bool
    publicHost: string
    publicIpv4: string
    publicIpv6: string
    regionId: int
    regionCode: string
    regionName: string
    derpPort: int
    verboseDiagnostics: bool

proc parseArgs(): Result[GatewayArgs, string] =
  var args = GatewayArgs(
    listenHost: "0.0.0.0",
    controlListenPort: int(NimTsnetQuicDefaultPort),
    relayListenPort: int(NimTsnetQuicRelayDefaultPort),
    controlUrl: "",
    certificateFile: "",
    privateKeyFile: "",
    openNetwork: false,
    publicHost: "",
    publicIpv4: "",
    publicIpv6: "",
    regionId: 901,
    regionCode: "sin",
    regionName: "Open QUIC Network",
    derpPort: 443,
    verboseDiagnostics: false
  )
  var parser = initOptParser(commandLineParams())
  while true:
    parser.next()
    case parser.kind
    of cmdEnd:
      break
    of cmdLongOption, cmdShortOption:
      case parser.key
      of "listen-host":
        args.listenHost = parser.val.strip()
      of "control-listen-port":
        try:
          args.controlListenPort = max(1, parser.val.parseInt())
        except CatchableError:
          return err("invalid --control-listen-port: " & parser.val)
      of "relay-listen-port":
        try:
          args.relayListenPort = max(1, parser.val.parseInt())
        except CatchableError:
          return err("invalid --relay-listen-port: " & parser.val)
      of "control-url":
        args.controlUrl = parser.val.strip()
      of "certificate-file":
        args.certificateFile = parser.val.strip()
      of "private-key-file":
        args.privateKeyFile = parser.val.strip()
      of "open-network":
        args.openNetwork = true
      of "public-host":
        args.publicHost = parser.val.strip()
      of "public-ipv4":
        args.publicIpv4 = parser.val.strip()
      of "public-ipv6":
        args.publicIpv6 = parser.val.strip()
      of "region-id":
        try:
          args.regionId = max(1, parser.val.parseInt())
        except CatchableError:
          return err("invalid --region-id: " & parser.val)
      of "region-code":
        args.regionCode = parser.val.strip()
      of "region-name":
        args.regionName = parser.val.strip()
      of "derp-port":
        try:
          args.derpPort = max(1, parser.val.parseInt())
        except CatchableError:
          return err("invalid --derp-port: " & parser.val)
      of "verbose-diagnostics":
        args.verboseDiagnostics = true
      else:
        return err("unknown option: --" & parser.key)
    of cmdArgument:
      return err("unexpected argument: " & parser.key)
  if args.controlUrl.len == 0 and not args.openNetwork:
    return err("missing --control-url")
  if args.certificateFile.len == 0:
    return err("missing --certificate-file")
  if args.privateKeyFile.len == 0:
    return err("missing --private-key-file")
  ok(args)

proc installDiagnosticsHook() =
  clearDiagnosticsHooks()
  registerDiagnosticsHook(
    proc(event: DiagnosticsEvent) {.gcsafe.} =
      try:
        stderr.writeLine(
          "[nim-msquic] kind=" & $event.kind &
          " handle=" & $cast[uint](event.handle) &
          " note=" & event.note
        )
        flushFile(stderr)
      except CatchableError:
        discard
  )

proc main() =
  let args = parseArgs().valueOr:
    stderr.writeLine(error)
    quit(2)

  chronos.setThreadDispatcher(newDispatcher())
  if args.verboseDiagnostics:
    installDiagnosticsHook()

  let certPem =
    try:
      readFile(args.certificateFile)
    except CatchableError as exc:
      stderr.writeLine("failed to read certificate file: " & exc.msg)
      quit(2)
  let keyPem =
    try:
      readFile(args.privateKeyFile)
    except CatchableError as exc:
      stderr.writeLine("failed to read private key file: " & exc.msg)
      quit(2)

  let controlGateway = TsnetQuicControlGateway.new(TsnetQuicControlGatewayConfig(
    listenHost: args.listenHost,
    listenPort: uint16(args.controlListenPort),
    controlUrl: args.controlUrl,
    upstreamTransport: if args.openNetwork: nil else: defaultControlTransport(),
    certificatePem: certPem,
    privateKeyPem: keyPem,
    openNetwork: args.openNetwork,
    publicHost: args.publicHost,
    publicIpv4: args.publicIpv4,
    publicIpv6: args.publicIpv6,
    regionId: args.regionId,
    regionCode: args.regionCode,
    regionName: args.regionName,
    derpPort: args.derpPort
  ))
  let controlStarted = controlGateway.start()
  if controlStarted.isErr():
    stderr.writeLine(controlStarted.error)
    quit(2)

  let relayGateway = TsnetQuicRelayGateway.new()
  let relayStarted = relayGateway.start(
    listenHost = args.listenHost,
    listenPort = uint16(args.relayListenPort),
    certificatePem = certPem,
    privateKeyPem = keyPem
  )
  if relayStarted.isErr():
    controlGateway.stop()
    stderr.writeLine(relayStarted.error)
    quit(2)

  echo $(
    %*{
      "ok": true,
      "listenHost": args.listenHost,
      "control": {
        "listenPort": controlGateway.boundPort(),
        "protocol": "nim_quic",
        "openNetwork": args.openNetwork
      },
      "relay": {
        "listenPort": relayGateway.boundPort,
        "protocol": "nim_quic_relay_async"
      }
    }
  )

  while true:
    try:
      poll()
    except CatchableError as exc:
      stderr.writeLine("nim_quic gateway service poll error: " & exc.msg)
      quit(1)

main()
