# SSTable tooling commands: inspection, export, compaction, ingest, verification.

import std/[algorithm, base64, math, options, os, sequtils, strformat, strutils, tables]
when compileOption("threads"):
  import std/threadpool

import pebble/core/types
import pebble/sstable
import pebble/tooling/cli_core

type
  TableDigest = object
    path: string
    size: int
    sections: seq[TableSection]
    props: TableProperties

  VerificationResult = object
    path: string
    ok: bool
    errors: seq[string]
    warnings: seq[string]
    entries: int

  StatsResult = object
    path: string
    props: TableProperties
    sizeBytes: int
    sectionBytes: CountTable[BlockKind]

proc fatal(msg: string) =
  raiseCliError(msg)

proc ensureFiles(paths: seq[string]) =
  if paths.len == 0:
    fatal("at least one SSTable path is required")
  for path in paths:
    if not path.fileExists():
      fatal("file does not exist: " & path)

proc readTable(path: string): TableReader =
  let data = readFile(path)
  newTableReader(data)

proc kindLabel(kind: BlockKind): string =
  case kind
  of blockData: "data"
  of blockIndex: "index"
  of blockFilter: "filter"
  of blockMeta: "meta"

proc formatKey(key: Key): string =
  key.toBytes().escape()

proc formatKeyOpt(opt: Option[Key]): string =
  if opt.isSome():
    return formatKey(opt.get())
  "-"

proc collectDigest(path: string): TableDigest =
  try:
    let reader = readTable(path)
    result = TableDigest(
      path: path,
      size: reader.tableSize(),
      sections: reader.sections(),
      props: reader.properties()
    )
  except CatchableError as err:
    fatal("failed to load SSTable (" & path & "): " & err.msg)

proc renderDigest(d: TableDigest) =
  echo "File: ", d.path
  echo "  Size: ", d.size, " bytes"
  var countByKind = initCountTable[BlockKind]()
  var bytesByKind = initTable[BlockKind, int]()
  for section in d.sections:
    countByKind.inc(section.kind)
    let current = if bytesByKind.hasKey(section.kind): bytesByKind[section.kind] else: 0
    bytesByKind[section.kind] = current + section.handle.length
  for kind in [blockData, blockIndex, blockFilter, blockMeta]:
    let count = countByKind.getOrDefault(kind, 0)
    let total = if bytesByKind.hasKey(kind): bytesByKind[kind] else: 0
    let label = kindLabel(kind)
    echo "  ", label, " blocks: ", count, " total bytes: ", total
  let props = d.props
  echo "  Entries: ", props.numEntries
  echo "  Raw key bytes: ", props.rawKeySize
  echo "  Raw value bytes: ", props.rawValueSize
  echo "  Smallest key: ", formatKeyOpt(props.smallestKey)
  echo "  Largest key: ", formatKeyOpt(props.largestKey)
  if props.bloomBitsPerKey.isSome():
    echo "  Bloom bits/key: ", props.bloomBitsPerKey.get()

proc handleLs(ctx: CliContext; args: seq[string]) =
  if args.len == 0 or args.anyIt(it in ["-h", "--help"]):
    let prefix = (ctx.commandPath & @["ls"]).join(" ")
    echo "Usage:"
    echo "  ", prefix, " <sstable>..."
    echo "List SSTable block layout and property summary."
    return
  ensureFiles(args)
  for path in args:
    let digest = collectDigest(path)
    renderDigest(digest)

proc formatKV(key: Key; value: string; hexMode: bool): string =
  let keyStr = if hexMode: key.toBytes().toHex() else: key.toBytes().escape()
  let valueStr = if hexMode: value.toHex() else: value.escape()
  keyStr & " : " & valueStr

proc handleDump(ctx: CliContext; args: seq[string]) =
  var files: seq[string] = @[]
  var limit = -1
  var hexMode = false
  for arg in args:
    if arg.startsWith("--limit=") or arg.startsWith("--count="):
      let raw = arg.split("=", 1)[1]
      try:
        limit = raw.parseInt()
      except ValueError:
        fatal("failed to parse limit: " & raw)
    elif arg == "--limit" or arg == "--count":
      fatal("use --limit=<N> or --count=<N>")
    elif arg == "--hex":
      hexMode = true
    elif arg in ["-h", "--help"]:
      let prefix = (ctx.commandPath & @["dump"]).join(" ")
      echo "Usage:"
      echo "  ", prefix, " [--limit=N|--count=N] [--hex] <sstable>..."
      echo "Print SSTable key/value pairs in order; defaults to escaped text output."
      return
    else:
      files.add(arg)
  ensureFiles(files)
  for path in files:
    var reader: TableReader
    try:
      reader = readTable(path)
    except CatchableError as err:
      fatal("failed to load SSTable (" & path & "): " & err.msg)
    echo "File: ", path
    var count = 0
    for kv in reader.scan():
      echo "  ", formatKV(kv.key, kv.value, hexMode)
      inc count
      if limit >= 0 and count >= limit:
        break
    if limit >= 0 and count >= limit:
      echo "  ...(truncated)"

proc parseOutputPath(args: var seq[string]): string =
  var output = ""
  var remaining: seq[string] = @[]
  for arg in args:
    if arg.startsWith("--output="):
      output = arg.split("=", 1)[1]
    elif arg == "--output":
      fatal("use --output=<path> to specify an output file")
    else:
      remaining.add(arg)
  args = remaining
  if output.len == 0:
    fatal("missing required flag: --output=<path>")
  let dir = splitFile(output).dir
  if dir.len > 0 and not dir.dirExists():
    createDir(dir)
  output

proc loadAllEntries(paths: seq[string]): seq[tuple[key: Key, value: string]] =
  var entries: seq[tuple[key: Key, value: string]] = @[]
  for path in paths:
    var reader: TableReader
    try:
      reader = readTable(path)
    except CatchableError as err:
      fatal("failed to load SSTable (" & path & "): " & err.msg)
    for kv in reader.scan():
      entries.add((kv.key, kv.value))
  entries.sort(proc (a, b: tuple[key: Key, value: string]): int =
    compareBytewise(a.key, b.key))
  if entries.len == 0:
    fatal("no entries found across input tables; aborting")
  var deduped: seq[tuple[key: Key, value: string]] = @[]
  for entry in entries:
    if deduped.len == 0:
      deduped.add(entry)
      continue
    let cmp = compareBytewise(deduped[^1].key, entry.key)
    if cmp == 0:
      deduped[^1] = entry
    elif cmp < 0:
      deduped.add(entry)
    else:
      fatal("input keys must be in ascending order")
  deduped

proc writeTable(entries: seq[tuple[key: Key, value: string]];
                output: string; blockSize = 32 * 1024; bloomBits = 10) =
  var builder = initTableBuilder(blockSize = blockSize, bloomBitsPerKey = bloomBits)
  for entry in entries:
    builder.add(entry.key, entry.value)
  let artifacts = builder.finish()
  writeFile(output, artifacts.image.buffer)
  echo "Wrote SSTable: ", output
  echo "  Entries: ", artifacts.properties.numEntries
  echo "  Raw key bytes: ", artifacts.properties.rawKeySize
  echo "  Raw value bytes: ", artifacts.properties.rawValueSize

proc parseIntOption(arg, name: string): int =
  try:
    return arg.parseInt()
  except ValueError:
    fatal("flag " & name & " requires an integer: " & arg)
    0

proc handleCompact(ctx: CliContext; args: seq[string]) =
  if args.len == 0 or args.anyIt(it in ["-h", "--help"]):
    let prefix = (ctx.commandPath & @["compact"]).join(" ")
    echo "Usage:"
    echo "  ", prefix, " --output=<sstable> [--block-size=bytes] [--bloom-bits=N] <sstable>..."
    echo "Merge multiple SSTables into a single table; later duplicates win."
    return
  var mutableArgs = args
  let output = parseOutputPath(mutableArgs)
  var blockSize = 32 * 1024
  var bloomBits = 10
  var inputs: seq[string] = @[]
  for arg in mutableArgs:
    if arg.startsWith("--block-size="):
      blockSize = parseIntOption(arg.split("=", 1)[1], "--block-size")
    elif arg.startsWith("--bloom-bits="):
      bloomBits = parseIntOption(arg.split("=", 1)[1], "--bloom-bits")
    else:
      inputs.add(arg)
  ensureFiles(inputs)
  let entries = loadAllEntries(inputs)
  writeTable(entries, output, blockSize, bloomBits)

proc parseEncoding(arg: string): string =
  let value = arg.toLowerAscii()
  case value
  of "plain", "base64", "hex":
    value
  else:
    fatal("unknown encoding: " & arg)
    ""

proc parseHexDigit(ch: char): int =
  case ch
  of '0'..'9': ord(ch) - ord('0')
  of 'a'..'f': ord(ch) - ord('a') + 10
  of 'A'..'F': ord(ch) - ord('A') + 10
  else: -1

proc decodeHex(value: string): string =
  if value.len mod 2 != 0:
    fatal("hex input length must be even")
  var buffer = newString(value.len div 2)
  for i in 0 ..< buffer.len:
    let hi = parseHexDigit(value[2 * i])
    let lo = parseHexDigit(value[2 * i + 1])
    if hi < 0 or lo < 0:
      fatal("hex input contains invalid characters")
    buffer[i] = chr((hi shl 4) or lo)
  buffer

proc decodeField(field, encoding: string): string =
  case encoding
  of "plain":
    field
  of "base64":
    try:
      decode(field)
    except CatchableError as err:
      fatal("failed to decode base64: " & err.msg)
      ""
  else:
    decodeHex(field)

proc parseLine(line: string): tuple[keyPart: string, valuePart: string] =
  let trimmed = line.strip()
  if trimmed.len == 0 or trimmed[0] == '#':
    return ("", "")
  var sep = trimmed.find('\t')
  if sep < 0:
    sep = trimmed.find('=')
  if sep < 0:
    fatal("line is missing a separator (use tab or '='): " & trimmed)
  let keyPart = trimmed[0 ..< sep]
  let valuePart = trimmed[(sep + 1) .. ^1]
  (keyPart, valuePart)

proc handleIngest(ctx: CliContext; args: seq[string]) =
  if args.len == 0 or args.anyIt(it in ["-h", "--help"]):
    let prefix = (ctx.commandPath & @["ingest"]).join(" ")
    echo "Usage:"
    echo "  ", prefix, " --output=<sstable> [--input=<data file|->] [--encoding=base64|plain|hex]",
         " [--block-size=bytes] [--bloom-bits=N] [--strict-order]"
    echo "Build an SSTable from a text/pipe stream; defaults to base64 key/value input."
    return
  var inputPath = "-"
  var outputPath = ""
  var encoding = "base64"
  var blockSize = 32 * 1024
  var bloomBits = 10
  var strictOrder = false
  var extras: seq[string] = @[]
  for arg in args:
    if arg.startsWith("--input="):
      inputPath = arg.split("=", 1)[1]
    elif arg.startsWith("--output="):
      outputPath = arg.split("=", 1)[1]
    elif arg.startsWith("--encoding="):
      encoding = parseEncoding(arg.split("=", 1)[1])
    elif arg.startsWith("--block-size="):
      blockSize = parseIntOption(arg.split("=", 1)[1], "--block-size")
    elif arg.startsWith("--bloom-bits="):
      bloomBits = parseIntOption(arg.split("=", 1)[1], "--bloom-bits")
    elif arg == "--strict-order":
      strictOrder = true
    else:
      extras.add(arg)
  if outputPath.len == 0:
    fatal("missing required flag: --output=<sstable>")
  if extras.len > 0 and inputPath == "-":
    inputPath = extras[0]
    if extras.len > 1:
      fatal("unexpected argument: " & extras[1])
  let dir = splitFile(outputPath).dir
  if dir.len > 0 and not dir.dirExists():
    createDir(dir)
  var builder = initTableBuilder(blockSize = blockSize, bloomBitsPerKey = bloomBits)
  var lastKey: Option[Key] = none(Key)
  var lineNo = 0
  var processed = 0
  proc ingestLine(line: string) =
    var pair = parseLine(line)
    if pair.keyPart.len == 0 and pair.valuePart.len == 0:
      return
    let keyBytes = decodeField(pair.keyPart, encoding)
    let valueBytes = decodeField(pair.valuePart, encoding)
    let key = keyBytes.toKey()
    if strictOrder and lastKey.isSome() and compareBytewise(lastKey.get(), key) >= 0:
      fatal(fmt"line {lineNo} is not strictly increasing: {formatKey(key)}")
    builder.add(key, valueBytes)
    lastKey = some(key)
    inc processed
  if inputPath == "-" or inputPath.len == 0:
    var line: string
    while stdin.readLine(line):
      inc lineNo
      ingestLine(line)
  else:
    var f: File
    if not f.open(inputPath, fmRead):
      fatal("failed to open input file: " & inputPath)
    defer:
      f.close()
    var line: string
    while f.readLine(line):
      inc lineNo
      ingestLine(line)
  if processed == 0:
    fatal("no key/value rows were ingested")
  let artifacts = builder.finish()
  writeFile(outputPath, artifacts.image.buffer)
  echo "Built SSTable: ", outputPath
  echo "  Lines processed: ", processed
  echo "  Entries: ", artifacts.properties.numEntries
  echo "  Smallest key: ", formatKeyOpt(artifacts.properties.smallestKey)
  echo "  Largest key: ", formatKeyOpt(artifacts.properties.largestKey)

proc verifyTable(path: string): VerificationResult =
  result = VerificationResult(path: path, ok: true, errors: @[], warnings: @[], entries: 0)
  try:
    let reader = readTable(path)
    var lastKey: Option[Key] = none(Key)
    var firstKey: Option[Key] = none(Key)
    for kv in reader.scan():
      inc result.entries
      if firstKey.isNone():
        firstKey = some(kv.key)
      if lastKey.isSome():
        if compareBytewise(lastKey.get(), kv.key) >= 0:
          result.errors.add(
            fmt"out-of-order key: prev={formatKey(lastKey.get())}, current={formatKey(kv.key)}")
      lastKey = some(kv.key)
    let props = reader.properties()
    if props.numEntries != result.entries:
      result.warnings.add(
        fmt"properties.numEntries={props.numEntries} mismatches actual count {result.entries}")
    if props.smallestKey.isSome() and firstKey.isSome():
      if compareBytewise(props.smallestKey.get(), firstKey.get()) != 0:
        result.warnings.add(
          fmt"smallest key property {formatKey(props.smallestKey.get())} does not match actual {formatKey(firstKey.get())}")
    if props.largestKey.isSome() and lastKey.isSome():
      if compareBytewise(props.largestKey.get(), lastKey.get()) != 0:
        result.warnings.add(
          fmt"largest key property {formatKey(props.largestKey.get())} does not match actual {formatKey(lastKey.get())}")
    result.ok = result.errors.len == 0
  except CatchableError as err:
    result.ok = false
    result.errors.add("failed to read SSTable: " & err.msg)

proc handleVerify(ctx: CliContext; args: seq[string]) =
  if args.len == 0 or args.anyIt(it in ["-h", "--help"]):
    let prefix = (ctx.commandPath & @["verify"]).join(" ")
    echo "Usage:"
    echo "  ", prefix, " <sstable>..."
    echo "Validate SSTable internal structure and property consistency."
    return
  ensureFiles(args)
  var hadError = false
  for path in args:
    let res = verifyTable(path)
    echo "File: ", path
    if res.ok:
      echo "  [OK] verification succeeded, entries: ", res.entries
    else:
      echo "  [FAIL] verification failed"
    if res.errors.len > 0:
      for err in res.errors:
        echo "    error: ", err
    if res.warnings.len > 0:
      for warn in res.warnings:
        echo "    warning: ", warn
    if not res.ok:
      hadError = true
  if hadError:
    fatal("verification failed")

proc collectStats(path: string): StatsResult =
  let reader = readTable(path)
  var counts = initCountTable[BlockKind]()
  for section in reader.sections():
    counts.inc(section.kind, section.handle.length)
  result = StatsResult(
    path: path,
    props: reader.properties(),
    sizeBytes: reader.tableSize(),
    sectionBytes: counts
  )

proc asciiBar(percent: float; width = 24): string =
  let clamped = max(0.0, min(100.0, percent))
  var filled = int(round(clamped / 100.0 * float(width)))
  if filled > width:
    filled = width
  let empty = width - filled
  "#" .repeat(filled) & ".".repeat(empty)

proc renderStats(stats: StatsResult) =
  echo "File: ", stats.path
  echo "  Size: ", stats.sizeBytes, " bytes"
  echo "  Entries: ", stats.props.numEntries
  let total = if stats.sizeBytes == 0: 1.0 else: float(stats.sizeBytes)
  for kind in [blockData, blockIndex, blockFilter, blockMeta]:
    let bytes = stats.sectionBytes.getOrDefault(kind, 0)
    let percent = float(bytes) / total * 100.0
    echo fmt"  {kindLabel(kind):<6} {bytes:>10} B  {percent:>6.2f}%  {asciiBar(percent)}"

when compileOption("threads"):
  type StatsFuture = FlowVar[tuple[success: bool, stats: StatsResult, err: string]]

  proc computeStatsAsync(path: string): tuple[success: bool, stats: StatsResult, err: string] =
    try:
      (true, collectStats(path), "")
    except CatchableError as err:
      (false, StatsResult(path: path), err.msg)

proc handleStats(ctx: CliContext; args: seq[string]) =
  var files: seq[string] = @[]
  var parallel = false
  for arg in args:
    if arg in ["-h", "--help"]:
      let prefix = (ctx.commandPath & @["stats"]).join(" ")
      echo "Usage:"
      echo "  ", prefix, " [--parallel] <sstable>..."
      echo "Summarize SSTable properties and render an ASCII breakdown."
      return
    elif arg == "--parallel":
      parallel = true
    else:
      files.add(arg)
  ensureFiles(files)
  var results: seq[StatsResult] = @[]
  if parallel:
    when compileOption("threads"):
      var futures: seq[StatsFuture] = @[]
      for path in files:
        futures.add(spawn computeStatsAsync(path))
      for fut in futures:
        let res = ^fut
        if not res.success:
          fatal("failed to compute stats (" & res.stats.path & "): " & res.err)
        results.add(res.stats)
    else:
      stderr.writeLine("threads are disabled at compile time; falling back to serial stats computation")
      parallel = false
  if not parallel:
    for path in files:
      try:
        results.add(collectStats(path))
      except CatchableError as err:
        fatal("failed to compute stats (" & path & "): " & err.msg)
  for item in results:
    renderStats(item)
  if results.len > 1:
    var totalProps = TableProperties()
    var totalSize = 0
    var totalCounts = initCountTable[BlockKind]()
    for item in results:
      totalProps.mergeProperties(item.props)
      totalSize += item.sizeBytes
      for kind, value in item.sectionBytes.pairs():
        totalCounts.inc(kind, value)
    echo "Summary:"
    echo "  Total size: ", totalSize, " bytes"
    echo "  Total entries: ", totalProps.numEntries
    let grand = if totalSize == 0: 1.0 else: float(totalSize)
    for kind in [blockData, blockIndex, blockFilter, blockMeta]:
      let bytes = totalCounts.getOrDefault(kind, 0)
      let percent = float(bytes) / grand * 100.0
      echo fmt"  {kindLabel(kind):<6} {bytes:>10} B  {percent:>6.2f}%  {asciiBar(percent)}"

proc registerSSTableCommands*(root: CliCommand) =
  let group = newCliCommand(
    name = "sstable",
    summary = "SSTable tooling suite",
    details = "Includes ls/dump/compact/ingest/verify/stats subcommands."
  )
  let lsCmd = newCliCommand("ls", "Summarize SSTable block layout", handleLs)
  lsCmd.addAlias("layout")
  let dumpCmd = newCliCommand("dump", "Stream key/value pairs", handleDump)
  dumpCmd.addAlias("scan")
  let compactCmd = newCliCommand("compact", "Merge multiple SSTables", handleCompact)
  let ingestCmd = newCliCommand("ingest", "Create an SSTable from text input", handleIngest)
  let verifyCmd = newCliCommand("verify", "Validate SSTable structure", handleVerify)
  verifyCmd.addAlias("check")
  let statsCmd = newCliCommand("stats", "Report SSTable size breakdown", handleStats)
  statsCmd.addAlias("space")
  statsCmd.addAlias("properties")
  group.addSubcommand(lsCmd)
  group.addSubcommand(dumpCmd)
  group.addSubcommand(compactCmd)
  group.addSubcommand(ingestCmd)
  group.addSubcommand(verifyCmd)
  group.addSubcommand(statsCmd)
  root.addSubcommand(group)
