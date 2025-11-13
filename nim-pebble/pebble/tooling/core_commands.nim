# Core CLI commands: module mapping, config export, etc.

import std/[strutils, tables]

import pebble/config/loader
import pebble/core/module_map
import pebble/tooling/cli_core

proc moduleMapHandler(ctx: CliContext; args: seq[string]) =
  if args.len > 0:
    raiseCliError("module-map does not accept positional arguments")
  for goPkg, nimModule in packageMap.pairs():
    echo goPkg, " -> ", nimModule

proc configDumpHandler(ctx: CliContext; args: seq[string]) =
  var path = ""
  for arg in args:
    if arg.startsWith("--path="):
      path = arg.split("=", 1)[1]
    elif arg == "--path":
      raiseCliError("use --path=<file> to specify the config path")
    elif path.len == 0:
      path = arg
    else:
      raiseCliError("unexpected argument: " & arg)
  try:
    var cfg = newFlatConfig()
    if path.len > 0:
      cfg = loadConfigFile(path)
    cfg.applyEnvOverrides()
    for key, value in cfg.pairs():
      echo key, "=", value
  except CatchableError as err:
    raiseCliError("failed to load config: " & err.msg)

proc registerCoreCommands*(root: CliCommand) =
  let moduleMapCmd = newCliCommand(
    name = "module-map",
    summary = "List the Go package to Nim module mapping",
    handler = moduleMapHandler
  )
  root.addSubcommand(moduleMapCmd)

  var configDetails = "Flags:\n"
  configDetails.add("  --path=<file>  Path to the JSON config to expand\n")
  configDetails.add("If omitted, only the default config with env overrides is printed.")

  let configDumpCmd = newCliCommand(
    name = "config-dump",
    summary = "Print flattened configuration key/value pairs",
    handler = configDumpHandler,
    details = configDetails
  )
  root.addSubcommand(configDumpCmd)
