import std/[json, options, os, parseopt, strutils]
import chronos

import ../libp2p/transports/tsnet/[control, quiccontrol]
import ../libp2p/utility

type
  GatewayArgs = object
    listenHost: string
    listenPort: int
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

proc parseArgs(): Result[GatewayArgs, string] =
  var args = GatewayArgs(
    listenHost: "0.0.0.0",
    listenPort: int(NimTsnetQuicDefaultPort),
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
    derpPort: 443
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
      of "listen-port":
        try:
          args.listenPort = max(1, parser.val.parseInt())
        except CatchableError:
          return err("invalid --listen-port: " & parser.val)
      of "control-url":
        args.controlUrl = parser.val.strip()
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
      of "certificate-file":
        args.certificateFile = parser.val.strip()
      of "private-key-file":
        args.privateKeyFile = parser.val.strip()
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

proc main() =
  let args = parseArgs().valueOr:
    stderr.writeLine(error)
    quit(2)
  chronos.setThreadDispatcher(newDispatcher())
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

  let gateway = TsnetQuicControlGateway.new(TsnetQuicControlGatewayConfig(
    listenHost: args.listenHost,
    listenPort: uint16(args.listenPort),
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
  let started = gateway.start()
  if started.isErr():
    stderr.writeLine(started.error)
    quit(2)
  echo $(
    %*{
      "ok": true,
      "listenHost": args.listenHost,
      "listenPort": gateway.boundPort(),
      "controlUrl": args.controlUrl,
      "openNetwork": args.openNetwork,
      "protocol": "nim_quic",
      "healthUrl": nimQuicHealthUrl(
        "https://" & args.listenHost & ":" & $gateway.boundPort()
      )
    }
  )
  while true:
    try:
      poll()
    except CatchableError as exc:
      stderr.writeLine("nim_quic gateway poll error: " & exc.msg)
      quit(1)

main()
