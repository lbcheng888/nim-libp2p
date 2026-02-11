# Generic CLI command registration and dispatch helpers.
# Provides nested subcommands, aliases, and usage rendering.

import std/[options, sequtils, strformat, strutils, tables]

type
  CliError* = object of CatchableError

type
  CliHandler* = proc (ctx: CliContext; args: seq[string]) {.gcsafe.}

  CliContext* = object
    ## Invocation context (program name and command path) used for messaging.
    programName*: string
    commandPath*: seq[string]

  CliCommand* = ref object
    name*: string
    summary*: string
    details*: string
    handler*: Option[CliHandler]
    subcommands*: OrderedTable[string, CliCommand]
    aliases*: seq[string]

proc raiseCliError*(msg: string) {.raises: [CliError].} =
  raise newException(CliError, msg)

proc newCliCommand*(name, summary: string; handler: CliHandler = nil;
                    details = ""): CliCommand =
  result = CliCommand(
    name: name,
    summary: summary,
    details: details,
    handler: none(CliHandler),
    subcommands: initOrderedTable[string, CliCommand](),
    aliases: @[]
  )
  if handler != nil:
    result.handler = some(handler)

proc addAlias*(cmd: CliCommand; alias: string) =
  if alias.len == 0:
    raise newException(ValueError, "alias must be a non-empty string")
  if alias == cmd.name:
    return
  if not cmd.aliases.contains(alias):
    cmd.aliases.add(alias)

proc addSubcommand*(parent: CliCommand; child: CliCommand) =
  if parent.subcommands.hasKey(child.name):
    raise newException(ValueError, fmt"duplicate command registration: {child.name}")
  parent.subcommands[child.name] = child

proc findSubcommand(parent: CliCommand; name: string): Option[CliCommand] =
  if parent.subcommands.hasKey(name):
    return some(parent.subcommands[name])
  for sub in parent.subcommands.values():
    if sub.aliases.anyIt(it == name):
      return some(sub)
  none(CliCommand)

proc renderUsage*(cmd: CliCommand; ctx: CliContext) =
  let path = if ctx.commandPath.len == 0: ctx.programName
             else: (ctx.commandPath & @[cmd.name]).join(" ")
  echo "Usage:"
  if cmd.handler.isSome() or cmd.subcommands.len == 0:
    echo "  ", path, " [flags]"
  if cmd.subcommands.len > 0:
    echo "  ", path, " <subcommand> [flags]"
    echo ""
    echo "Subcommands:"
    for name, sub in cmd.subcommands:
      var aliases = ""
      if sub.aliases.len > 0:
        aliases = " (aliases: " & sub.aliases.join(", ") & ")"
      echo "  ", name, aliases
      if sub.summary.len > 0:
        echo "    ", sub.summary
    if cmd.details.len > 0:
      echo ""
      echo cmd.details

proc dispatch*(cmd: CliCommand; args: seq[string];
               ctx: CliContext = CliContext(programName: "pebble_cli",
                                            commandPath: @[])) =
  ## Recursively dispatch commands; render usage when no handler or --help observed.
  if args.len == 0:
    if cmd.handler.isSome():
      cmd.handler.get()(ctx, @[])
      return
    cmd.renderUsage(ctx)
    return
  if args[0] in ["-h", "--help", "help"]:
    cmd.renderUsage(ctx)
    return
  let sub = cmd.findSubcommand(args[0])
  if sub.isSome():
    var nextCtx = ctx
    nextCtx.commandPath = ctx.commandPath & @[cmd.name]
    let tail = if args.len > 1: args[1 ..< args.len] else: @[]
    dispatch(sub.get(), tail, nextCtx)
    return
  if cmd.handler.isSome():
    cmd.handler.get()(ctx, args)
  else:
    raiseCliError("unknown command: " & args[0])
