# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import chronos, chronicles
import metrics/chronos_httpserver

import ../switch
import ../metrics_exporter
import ../protocols/kademlia/kademlia

logScope:
  topics = "libp2p metrics"

type
  MetricsService* = ref object of Service
    address*: string
    port*: Port
    interval*: chronos.Duration
    ipnsTrackers*: seq[KadDHT]
    server: MetricsHttpServerRef
    metricsLoop: Future[void]
    bitswapState: BitswapMetricsState
    ipnsState: IpnsMetricsState
    switchState: SwitchMetricsState

proc new*(
    T: typedesc[MetricsService],
    address: string = "127.0.0.1",
    port: Port = Port(8000),
    interval: chronos.Duration = chronos.seconds(5),
): T =
  T(
    address: address,
    port: port,
    interval: interval,
    bitswapState: initBitswapMetricsState(),
    ipnsState: initIpnsMetricsState(),
    switchState: initSwitchMetricsState(),
  )

method setup*(
    self: MetricsService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(self).setup(switch)
  if hasBeenSetup:
    await self.run(switch)
  hasBeenSetup

method run*(
    self: MetricsService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  if isNil(self.metricsLoop):
    updateSwitchMetrics(switch, self.switchState)
    try:
      await updateBitswapMetrics(switch, self.bitswapState)
    except CatchableError as exc:
      warn "failed to update Bitswap metrics", error = exc.msg
    when not defined(libp2p_disable_datatransfer):
      try:
        await updateDataTransferMetrics(switch)
      except CatchableError as exc:
        warn "failed to update DataTransfer metrics", error = exc.msg
    if self.ipnsTrackers.len > 0:
      updateIpnsMetrics(self.ipnsTrackers, self.ipnsState)
    else:
      updateIpnsMetrics(@[], self.ipnsState)
    self.metricsLoop = spawnMetricsLoop(
      switch,
      self.interval,
      proc(): seq[KadDHT] {.gcsafe.} =
        self.ipnsTrackers,
    )

  when defined(metrics):
    if isNil(self.server):
      let serverRes = MetricsHttpServerRef.new(self.address, self.port)
      if serverRes.isErr:
        warn "failed to initialize metrics HTTP server",
          address = self.address, port = int(self.port), error = $serverRes.error
      else:
        self.server = serverRes.get()
        try:
          await self.server.start()
          info "Metrics HTTP server started", address = self.address, port = int(self.port)
        except MetricsError as exc:
          warn "failed to start metrics HTTP server",
            address = self.address, port = int(self.port), error = exc.msg
          try:
            await self.server.close()
          except CatchableError as closeErr:
            trace "failed to close metrics HTTP server", error = closeErr.msg
          self.server = nil
        except CatchableError as exc:
          warn "exception while starting metrics HTTP server",
            address = self.address, port = int(self.port), error = exc.msg
          try:
            await self.server.close()
          except CatchableError as closeErr:
            trace "failed to close metrics HTTP server", error = closeErr.msg
          self.server = nil
  else:
    debug "metrics disabled at compile time; skipping HTTP exporter"

method stop*(
    self: MetricsService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(self).stop(switch)
  if hasBeenStopped:
    if not isNil(self.metricsLoop):
      await self.metricsLoop.cancelAndWait()
      self.metricsLoop = nil
      self.bitswapState = initBitswapMetricsState()
      self.ipnsState = initIpnsMetricsState()
      self.switchState = initSwitchMetricsState()

    when defined(metrics):
      if not isNil(self.server):
        try:
          await self.server.stop()
        except MetricsError as exc:
          trace "failed to stop metrics HTTP server", error = exc.msg
        except CatchableError as exc:
          trace "exception while stopping metrics HTTP server", error = exc.msg
        try:
          await self.server.close()
        except CatchableError as exc:
          trace "exception while closing metrics HTTP server", error = exc.msg
        self.server = nil
    updateIpnsMetrics(@[], self.ipnsState)
  hasBeenStopped

proc registerIpnsTracker*(
    self: MetricsService, kad: KadDHT
) {.public.} =
  if kad.isNil:
    return
  if kad notin self.ipnsTrackers:
    self.ipnsTrackers.add(kad)

{.pop.}
