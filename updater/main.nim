## nim-libp2p updater CLI entrypoint

import std/[os, strformat, strutils]
import chronos
import chronicles
import pkg/results

import ./config
import ./manifest
import ./updater

var ctrlStopEventAddr: pointer

proc handleCtrlC() {.noconv.} =
  if ctrlStopEventAddr != nil:
    cast[AsyncEvent](ctrlStopEventAddr).fire()

proc run(): Future[void] {.async.} =
  let args = commandLineParams()
  let configPath = if args.len > 0: args[0] else: ""

  let cfgRes = loadUpdaterConfig(configPath)
  if cfgRes.isErr:
    stderr.writeLine("failed to load config: " & cfgRes.error)
    quit(1)
  let cfg = cfgRes.get()

  let updaterRes = newUpdater(cfg)
  if updaterRes.isErr:
    stderr.writeLine("failed to initialize updater: " & updaterRes.error)
    quit(1)
  let updater = updaterRes.get()

  updater.onManifest(
    proc(_: Updater, manifest: SignedManifest) {.async.} =
      info "manifest updated",
        channel = manifest.data.channel,
        version = manifest.data.version,
        sequence = manifest.data.sequence,
        manifestCid = manifest.data.manifestCid
  )

  let stopEvent = newAsyncEvent()
  ctrlStopEventAddr = cast[pointer](stopEvent)
  setControlCHook(handleCtrlC)

  info "starting updater..."
  try:
    await updater.start()
  except CatchableError as exc:
    stderr.writeLine("failed to start: " & exc.msg)
    await updater.stop()
    quit(1)

  info "updater started; press Ctrl+C to exit"

  await stopEvent.wait()

  info "shutdown signal received; stopping updater"
  await updater.stop()

when isMainModule:
  waitFor(run())
