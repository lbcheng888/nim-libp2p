import std/[os, osproc, sequtils, strutils]

type
  Options = object
    op: string
    sshHost: string
    sshUser: string
    sshPort: int
    lines: int
    remoteRepo: string
    remoteNim: string
    localRepo: string
    files: seq[string]
    pretty: bool

proc parseArgs(): Options =
  result = Options(
    sshPort: 22,
    lines: 200,
    remoteRepo: "/root/nim-libp2p",
    remoteNim: "/root/Nim/bin/nim",
    localRepo: "/Users/lbcheng/nim-libp2p"
  )
  let args = commandLineParams()
  var i = 0
  while i < args.len:
    let arg = args[i]
    if arg == "--pretty":
      result.pretty = true
      inc i
    elif arg.startsWith("--op="):
      result.op = arg.split("=", 1)[1]
      inc i
    elif arg == "--op":
      result.op = args[i + 1]
      inc i, 2
    elif arg.startsWith("--ssh-host="):
      result.sshHost = arg.split("=", 1)[1]
      inc i
    elif arg == "--ssh-host":
      result.sshHost = args[i + 1]
      inc i, 2
    elif arg.startsWith("--ssh-user="):
      result.sshUser = arg.split("=", 1)[1]
      inc i
    elif arg == "--ssh-user":
      result.sshUser = args[i + 1]
      inc i, 2
    elif arg.startsWith("--ssh-port="):
      result.sshPort = parseInt(arg.split("=", 1)[1])
      inc i
    elif arg == "--ssh-port":
      result.sshPort = parseInt(args[i + 1])
      inc i, 2
    elif arg.startsWith("--lines="):
      result.lines = max(1, parseInt(arg.split("=", 1)[1]))
      inc i
    elif arg == "--lines":
      result.lines = max(1, parseInt(args[i + 1]))
      inc i, 2
    elif arg.startsWith("--remote-repo="):
      result.remoteRepo = arg.split("=", 1)[1]
      inc i
    elif arg == "--remote-repo":
      result.remoteRepo = args[i + 1]
      inc i, 2
    elif arg.startsWith("--remote-nim="):
      result.remoteNim = arg.split("=", 1)[1]
      inc i
    elif arg == "--remote-nim":
      result.remoteNim = args[i + 1]
      inc i, 2
    elif arg.startsWith("--local-repo="):
      result.localRepo = arg.split("=", 1)[1]
      inc i
    elif arg == "--local-repo":
      result.localRepo = args[i + 1]
      inc i, 2
    elif arg.startsWith("--file="):
      result.files.add(arg.split("=", 1)[1])
      inc i
    elif arg == "--file":
      result.files.add(args[i + 1])
      inc i, 2
    else:
      raise newException(ValueError, "unknown option: " & arg)

  if result.op.len == 0:
    raise newException(ValueError, "--op is required")
  if result.sshHost.len == 0:
    raise newException(ValueError, "--ssh-host is required")
  if result.sshUser.len == 0:
    raise newException(ValueError, "--ssh-user is required")

proc defaultFiles(): seq[string] =
  @[
    "examples/mobile_ffi/libnimlibp2p.nim",
    "examples/mobile_ffi/tsnet_product_node.nim"
  ]

proc defaultGatewayFiles(): seq[string] =
  @[
    "examples/tsnet_quic_gateway_service.nim",
    "libp2p/transports/msquicstream.nim",
    "libp2p/transports/tsnet/control.nim",
    "libp2p/transports/tsnet/quiccontrol.nim",
    "libp2p/transports/tsnet/quicrelay.nim"
  ]

proc remoteTarget(opts: Options): string =
  opts.sshUser & "@" & opts.sshHost

proc runCmd(exe: string; args: seq[string]): string =
  let res = execCmdEx(
    quoteShellCommand(@[exe] & args),
    options = {poUsePath, poStdErrToStdOut}
  )
  if res.exitCode != 0:
    let msg =
      if res.output.strip().len > 0: res.output.strip()
      else: exe & " failed"
    raise newException(IOError, msg)
  res.output

proc sshRun(opts: Options; remoteCmd: string): string =
  var args = newSeq[string]()
  if opts.sshPort != 22:
    args.add(@["-p", $opts.sshPort])
  args.add(remoteTarget(opts))
  args.add(remoteCmd)
  runCmd("ssh", args)

proc scpPut(opts: Options; localPath, remotePath: string) =
  var args = newSeq[string]()
  if opts.sshPort != 22:
    args.add(@["-P", $opts.sshPort])
  args.add(localPath)
  args.add(remoteTarget(opts) & ":" & remotePath)
  discard runCmd("scp", args)

proc syncFiles(opts: Options; files: seq[string]) =
  for rel in files:
    let localPath = opts.localRepo / rel
    let remotePath = opts.remoteRepo / rel
    if not fileExists(localPath):
      raise newException(IOError, "missing local file: " & localPath)
    discard sshRun(opts, "mkdir -p " & quoteShell(remotePath.parentDir()))
    scpPut(opts, localPath, remotePath)
    echo "synced ", rel

proc syncProduct(opts: Options) =
  let files =
    if opts.files.len > 0: opts.files
    else: defaultFiles()
  syncFiles(opts, files)

proc syncGateway(opts: Options) =
  let files =
    if opts.files.len > 0: opts.files
    else: defaultGatewayFiles()
  syncFiles(opts, files)

proc buildProduct(opts: Options) =
  let cmd =
    "cd " & quoteShell(opts.remoteRepo) & " && " &
    quoteShell(opts.remoteNim) &
    " c --hints:off --warnings:off -d:ssl -d:libp2p_msquic_experimental -d:libp2p_msquic_builtin " &
    "-o:" & quoteShell(opts.remoteRepo / "build/tsnet_product_node") & " " &
    quoteShell(opts.remoteRepo / "examples/mobile_ffi/tsnet_product_node.nim")
  let output = sshRun(opts, cmd)
  if output.strip().len > 0:
    echo output.strip()
  let statOut = sshRun(opts, "stat -c '%y %s' " & quoteShell(opts.remoteRepo / "build/tsnet_product_node"))
  echo statOut.strip()

proc buildGateway(opts: Options) =
  let cmd =
    "cd " & quoteShell(opts.remoteRepo) & " && " &
    quoteShell(opts.remoteNim) &
    " c --hints:off --warnings:off -d:ssl -d:libp2p_msquic_experimental -d:libp2p_msquic_builtin " &
    "-o:" & quoteShell(opts.remoteRepo / "build/tsnet_quic_gateway_service") & " " &
    quoteShell(opts.remoteRepo / "examples/tsnet_quic_gateway_service.nim")
  let output = sshRun(opts, cmd)
  if output.strip().len > 0:
    echo output.strip()
  let statOut = sshRun(opts, "stat -c '%y %s' " & quoteShell(opts.remoteRepo / "build/tsnet_quic_gateway_service"))
  echo statOut.strip()

proc restartProduct(opts: Options) =
  let output = sshRun(
    opts,
    "systemctl restart nim-tsnet-product.service && " &
    "systemctl show nim-tsnet-product.service -p MainPID -p ExecMainStartTimestamp -p ActiveState -p SubState -p NRestarts"
  )
  echo output.strip()

proc restartGateway(opts: Options) =
  let output = sshRun(
    opts,
    "systemctl restart nim-tsnet-gateway.service && " &
    "systemctl show nim-tsnet-gateway.service -p MainPID -p ExecMainStartTimestamp -p ActiveState -p SubState -p NRestarts"
  )
  echo output.strip()

proc statusProduct(opts: Options) =
  let output = sshRun(
    opts,
    "systemctl show nim-tsnet-product.service -p MainPID -p ExecMainStartTimestamp -p ActiveState -p SubState -p NRestarts && " &
    "stat -c '%y %s' " & quoteShell(opts.remoteRepo / "build/tsnet_product_node")
  )
  echo output.strip()

proc statusGateway(opts: Options) =
  let output = sshRun(
    opts,
    "systemctl show nim-tsnet-gateway.service -p MainPID -p ExecMainStartTimestamp -p ActiveState -p SubState -p NRestarts && " &
    "stat -c '%y %s' " & quoteShell(opts.remoteRepo / "build/tsnet_quic_gateway_service")
  )
  echo output.strip()

proc journalGateway(opts: Options) =
  let output = sshRun(
    opts,
    "journalctl -u nim-tsnet-gateway.service -n " & $opts.lines & " --no-pager"
  )
  if output.strip().len > 0:
    echo output.strip()

proc main() =
  let opts = parseArgs()
  case opts.op
  of "sync-product":
    syncProduct(opts)
  of "build-product":
    buildProduct(opts)
  of "restart-product":
    restartProduct(opts)
  of "status-product":
    statusProduct(opts)
  of "deploy-product":
    syncProduct(opts)
    buildProduct(opts)
    restartProduct(opts)
    statusProduct(opts)
  of "sync-gateway":
    syncGateway(opts)
  of "build-gateway":
    buildGateway(opts)
  of "restart-gateway":
    restartGateway(opts)
  of "status-gateway":
    statusGateway(opts)
  of "journal-gateway":
    journalGateway(opts)
  of "deploy-gateway":
    syncGateway(opts)
    buildGateway(opts)
    restartGateway(opts)
    statusGateway(opts)
  else:
    raise newException(ValueError, "unsupported op: " & opts.op)

when isMainModule:
  main()
