import std/os

import pebble/tooling/cli_core
import pebble/tooling/core_commands
import pebble/tooling/sstable_commands
import pebble/tooling/kv_commands

proc buildRootCommand*(): CliCommand =
  result = newCliCommand(
    name = "pebble_cli",
    summary = "Pebble Nim tooling entry point",
    details = "Invoke subcommands via `pebble_cli <command>` or append --help for usage."
  )
  registerCoreCommands(result)
  registerSSTableCommands(result)
  registerKvCommands(result)

when isMainModule:
  let root = buildRootCommand()
  let args = commandLineParams()
  try:
    dispatch(root, args, CliContext(programName: root.name, commandPath: @[]))
  except CliError as err:
    stderr.writeLine(err.msg)
    quit(QuitFailure)
