import std/[json, os, parseopt, strutils]
import chronos

import ../libp2p/transports/tsnet/quicrelay
import ../libp2p/utility

type
  GatewayArgs = object
    listenHost: string
    listenPort: int
    certificateFile: string
    privateKeyFile: string

proc parseArgs(): Result[GatewayArgs, string] =
  var args = GatewayArgs(
    listenHost: "0.0.0.0",
    listenPort: int(NimTsnetQuicRelayDefaultPort),
    certificateFile: "",
    privateKeyFile: ""
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
      of "certificate-file":
        args.certificateFile = parser.val.strip()
      of "private-key-file":
        args.privateKeyFile = parser.val.strip()
      else:
        return err("unknown option: --" & parser.key)
    of cmdArgument:
      return err("unexpected argument: " & parser.key)
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
  let gateway = TsnetQuicRelayGateway.new()
  let started = gateway.start(
    listenHost = args.listenHost,
    listenPort = uint16(args.listenPort),
    certificatePem = certPem,
    privateKeyPem = keyPem
  )
  if started.isErr():
    stderr.writeLine(started.error)
    quit(2)
  echo $(
    %*{
      "ok": true,
      "listenHost": args.listenHost,
      "listenPort": gateway.boundPort,
      "protocol": "nim_quic_relay_async"
    }
  )
  while true:
    try:
      poll()
    except CatchableError as exc:
      stderr.writeLine("nim_quic relay gateway poll error: " & exc.msg)
      quit(1)

main()
