import std/[json, net, os, osproc, sequtils, strutils]

const
  MaxFrameBytes = 16 * 1024 * 1024
  DefaultSshRpcBin = "/root/nim-libp2p/build/tsnet_product_rpc"

type
  RpcOptions = object
    host: string
    port: int
    timeoutMs: int
    pretty: bool
    payloadJson: string
    op: string
    peerId: string
    fromPeerId: string
    bodyContains: string
    text: string
    messageId: string
    replyTo: string
    requestAck: bool
    requestAckSet: bool
    maxEvents: int
    listenAddrs: seq[string]
    listenAddrsJson: string
    sshHost: string
    sshUser: string
    sshPort: int
    sshRpcBin: string

proc decodeFrameLength(header: string): int =
  if header.len != 4:
    return -1
  (header[0].ord shl 24) or (header[1].ord shl 16) or
    (header[2].ord shl 8) or header[3].ord

proc encodeFrameLength(size: int): string =
  result = newString(4)
  result[0] = char((size shr 24) and 0xFF)
  result[1] = char((size shr 16) and 0xFF)
  result[2] = char((size shr 8) and 0xFF)
  result[3] = char(size and 0xFF)

proc recvExact(client: Socket; size: int; timeoutMs: int): string =
  result = ""
  while result.len < size:
    let chunk = client.recv(size - result.len, timeout = timeoutMs)
    if chunk.len == 0:
      raise newException(IOError, "socket closed while reading")
    result.add(chunk)

proc sendExact(client: Socket; payload: string) =
  if payload.len == 0:
    return
  var written = 0
  while written < payload.len:
    let sent = client.send(unsafeAddr payload[written], payload.len - written)
    if sent <= 0:
      raise newException(IOError, "socket closed while writing")
    written += sent

proc parseBoolText(value: string): bool =
  case value.toLowerAscii()
  of "1", "true", "yes", "on":
    true
  of "0", "false", "no", "off":
    false
  else:
    raise newException(ValueError, "invalid bool value: " & value)

proc quotePosix(value: string): string =
  if value.len == 0:
    return "''"
  result = "'"
  for ch in value:
    if ch == '\'':
      result.add("'\"'\"'")
    else:
      result.add(ch)
  result.add('\'')

proc parseArgs(): RpcOptions =
  result = RpcOptions(
    host: "127.0.0.1",
    port: 0,
    timeoutMs: 30000,
    pretty: false,
    requestAck: true,
    sshPort: 22,
    sshRpcBin: DefaultSshRpcBin
  )
  let args = commandLineParams()
  var i = 0
  while i < args.len:
    let arg = args[i]
    if arg == "--pretty":
      result.pretty = true
      inc i
      continue
    if arg.startsWith("--host="):
      result.host = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--host":
      if i + 1 >= args.len:
        raise newException(ValueError, "--host requires a value")
      result.host = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--port="):
      result.port = parseInt(arg.split("=", 1)[1])
      inc i
      continue
    if arg == "--port":
      if i + 1 >= args.len:
        raise newException(ValueError, "--port requires a value")
      result.port = parseInt(args[i + 1])
      inc(i, 2)
      continue
    if arg.startsWith("--timeoutMs="):
      result.timeoutMs = max(1, parseInt(arg.split("=", 1)[1]))
      inc i
      continue
    if arg == "--timeoutMs":
      if i + 1 >= args.len:
        raise newException(ValueError, "--timeoutMs requires a value")
      result.timeoutMs = max(1, parseInt(args[i + 1]))
      inc(i, 2)
      continue
    if arg.startsWith("--op="):
      result.op = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--op":
      if i + 1 >= args.len:
        raise newException(ValueError, "--op requires a value")
      result.op = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--peer-id="):
      result.peerId = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--peer-id":
      if i + 1 >= args.len:
        raise newException(ValueError, "--peer-id requires a value")
      result.peerId = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--from-peer-id="):
      result.fromPeerId = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--from-peer-id":
      if i + 1 >= args.len:
        raise newException(ValueError, "--from-peer-id requires a value")
      result.fromPeerId = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--body-contains="):
      result.bodyContains = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--body-contains":
      if i + 1 >= args.len:
        raise newException(ValueError, "--body-contains requires a value")
      result.bodyContains = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--text="):
      result.text = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--text":
      if i + 1 >= args.len:
        raise newException(ValueError, "--text requires a value")
      result.text = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--message-id="):
      result.messageId = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--message-id":
      if i + 1 >= args.len:
        raise newException(ValueError, "--message-id requires a value")
      result.messageId = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--reply-to="):
      result.replyTo = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--reply-to":
      if i + 1 >= args.len:
        raise newException(ValueError, "--reply-to requires a value")
      result.replyTo = args[i + 1]
      inc(i, 2)
      continue
    if arg == "--request-ack":
      result.requestAck = true
      result.requestAckSet = true
      inc i
      continue
    if arg == "--no-request-ack":
      result.requestAck = false
      result.requestAckSet = true
      inc i
      continue
    if arg.startsWith("--request-ack="):
      result.requestAck = parseBoolText(arg.split("=", 1)[1])
      result.requestAckSet = true
      inc i
      continue
    if arg.startsWith("--max-events="):
      result.maxEvents = max(1, parseInt(arg.split("=", 1)[1]))
      inc i
      continue
    if arg == "--max-events":
      if i + 1 >= args.len:
        raise newException(ValueError, "--max-events requires a value")
      result.maxEvents = max(1, parseInt(args[i + 1]))
      inc(i, 2)
      continue
    if arg.startsWith("--listen-addr="):
      result.listenAddrs.add(arg.split("=", 1)[1])
      inc i
      continue
    if arg == "--listen-addr":
      if i + 1 >= args.len:
        raise newException(ValueError, "--listen-addr requires a value")
      result.listenAddrs.add(args[i + 1])
      inc(i, 2)
      continue
    if arg.startsWith("--listen-addrs-json="):
      result.listenAddrsJson = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--listen-addrs-json":
      if i + 1 >= args.len:
        raise newException(ValueError, "--listen-addrs-json requires a value")
      result.listenAddrsJson = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--ssh-host="):
      result.sshHost = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--ssh-host":
      if i + 1 >= args.len:
        raise newException(ValueError, "--ssh-host requires a value")
      result.sshHost = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--ssh-user="):
      result.sshUser = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--ssh-user":
      if i + 1 >= args.len:
        raise newException(ValueError, "--ssh-user requires a value")
      result.sshUser = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--ssh-port="):
      result.sshPort = max(1, parseInt(arg.split("=", 1)[1]))
      inc i
      continue
    if arg == "--ssh-port":
      if i + 1 >= args.len:
        raise newException(ValueError, "--ssh-port requires a value")
      result.sshPort = max(1, parseInt(args[i + 1]))
      inc(i, 2)
      continue
    if arg.startsWith("--ssh-rpc-bin="):
      result.sshRpcBin = arg.split("=", 1)[1]
      inc i
      continue
    if arg == "--ssh-rpc-bin":
      if i + 1 >= args.len:
        raise newException(ValueError, "--ssh-rpc-bin requires a value")
      result.sshRpcBin = args[i + 1]
      inc(i, 2)
      continue
    if arg.startsWith("--"):
      raise newException(ValueError, "unknown option: " & arg)
    if result.payloadJson.len == 0:
      result.payloadJson = arg
    else:
      raise newException(ValueError, "unexpected extra argument: " & arg)
    inc i
  if result.port <= 0:
    raise newException(ValueError, "--port is required")
  if result.payloadJson.len == 0 and result.op.len == 0:
    raise newException(ValueError, "需要传原始 payload json 或 --op")

proc buildPayload(opts: RpcOptions): string =
  if opts.payloadJson.len > 0:
    discard parseJson(opts.payloadJson)
    return opts.payloadJson
  var payload = %*{"op": opts.op}
  if opts.peerId.len > 0:
    payload["peerId"] = %opts.peerId
  if opts.fromPeerId.len > 0:
    payload["fromPeerId"] = %opts.fromPeerId
  if opts.bodyContains.len > 0:
    payload["bodyContains"] = %opts.bodyContains
  if opts.text.len > 0:
    payload["text"] = %opts.text
  if opts.messageId.len > 0:
    payload["messageId"] = %opts.messageId
  if opts.replyTo.len > 0:
    payload["replyTo"] = %opts.replyTo
  if opts.requestAckSet:
    payload["requestAck"] = %opts.requestAck
  if opts.maxEvents > 0:
    payload["maxEvents"] = %opts.maxEvents
  if opts.listenAddrsJson.len > 0:
    payload["listenAddrs"] = parseJson(opts.listenAddrsJson)
  elif opts.listenAddrs.len > 0:
    payload["listenAddrs"] = %opts.listenAddrs
  payload["timeoutMs"] = %opts.timeoutMs
  $payload

proc runLocalRpc(opts: RpcOptions; body: string): string =
  let ioTimeoutMs = max(opts.timeoutMs + 5_000, 5_000)
  var client = newSocket()
  client.connect(opts.host, Port(opts.port))
  defer:
    client.close()

  sendExact(client, encodeFrameLength(body.len))
  sendExact(client, body)

  let header = recvExact(client, 4, ioTimeoutMs)
  let payloadLen = decodeFrameLength(header)
  if payloadLen < 0 or payloadLen > MaxFrameBytes:
    raise newException(IOError, "invalid response frame length")
  if payloadLen == 0:
    return ""
  recvExact(client, payloadLen, ioTimeoutMs)

proc remoteRpcArgs(opts: RpcOptions): seq[string] =
  result = @[
    "--host", "127.0.0.1",
    "--port", $opts.port,
    "--timeoutMs", $opts.timeoutMs
  ]
  if opts.pretty:
    result.add("--pretty")
  if opts.payloadJson.len > 0:
    result.add(opts.payloadJson)
    return
  result.add(@["--op", opts.op])
  if opts.peerId.len > 0:
    result.add(@["--peer-id", opts.peerId])
  if opts.fromPeerId.len > 0:
    result.add(@["--from-peer-id", opts.fromPeerId])
  if opts.bodyContains.len > 0:
    result.add(@["--body-contains", opts.bodyContains])
  if opts.text.len > 0:
    result.add(@["--text", opts.text])
  if opts.messageId.len > 0:
    result.add(@["--message-id", opts.messageId])
  if opts.replyTo.len > 0:
    result.add(@["--reply-to", opts.replyTo])
  if opts.requestAckSet:
    result.add(if opts.requestAck: "--request-ack" else: "--no-request-ack")
  if opts.maxEvents > 0:
    result.add(@["--max-events", $opts.maxEvents])
  if opts.listenAddrsJson.len > 0:
    result.add(@["--listen-addrs-json", opts.listenAddrsJson])
  else:
    for addr in opts.listenAddrs:
      result.add(@["--listen-addr", addr])

proc runRemoteRpc(opts: RpcOptions): string =
  let target =
    if opts.sshUser.len > 0:
      opts.sshUser & "@" & opts.sshHost
    else:
      opts.sshHost
  let remoteCommand =
    quotePosix(opts.sshRpcBin) & " " &
    remoteRpcArgs(opts).map(quotePosix).join(" ")
  var sshArgs: seq[string] = @[]
  if opts.sshPort != 22:
    sshArgs.add(@["-p", $opts.sshPort])
  sshArgs.add(target)
  sshArgs.add(remoteCommand)
  let res = execCmdEx(
    "ssh " & sshArgs.map(quotePosix).join(" "),
    options = {poUsePath, poStdErrToStdOut}
  )
  if res.exitCode != 0:
    let errText = res.output.strip()
    raise newException(
      IOError,
      "ssh rpc failed" &
        (if errText.len > 0: ": " & errText else: "")
    )
  res.output

proc main() =
  let opts = parseArgs()
  let reply =
    if opts.sshHost.len > 0:
      runRemoteRpc(opts)
    else:
      runLocalRpc(opts, buildPayload(opts))
  let decoded = parseJson(reply)
  if opts.pretty:
    stdout.writeLine(pretty(decoded))
  else:
    stdout.writeLine($decoded)

when isMainModule:
  main()
