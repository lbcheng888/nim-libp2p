## Nim MsQuic E5 示例：验证 API 语义兼容与数据报能力。

import std/strformat

import ../../tooling/sample_tool

type
  SampleOutcome = object
    name: string
    success: bool
    datagramParity: bool

proc runScenario(enableDatagram: bool; peer: string): SampleOutcome =
  var clientConf = initSampleConfig(srClient, peer = peer & "-server")
  var serverConf = initSampleConfig(srServer, peer = peer & "-client")
  clientConf.enableDatagram = enableDatagram
  serverConf.enableDatagram = enableDatagram
  let clientRes = runSample(clientConf)
  let serverRes = runSample(serverConf)
  SampleOutcome(
    name: if enableDatagram: "datagram-enabled" else: "baseline",
    success: clientRes.success and serverRes.success,
    datagramParity: clientRes.datagramEnabled == serverRes.datagramEnabled)

proc report(outcome: SampleOutcome) =
  echo fmt"[{outcome.name}] success={outcome.success} datagramParity={outcome.datagramParity}"

proc verifySample(): bool =
  let baseline = runScenario(false, "nim-e5")
  report(baseline)
  if not baseline.success or not baseline.datagramParity:
    return false
  let datagram = runScenario(true, "nim-e5")
  report(datagram)
  baseline.success and datagram.success and datagram.datagramParity

when isMainModule:
  if verifySample():
    echo "E5 sample verification succeeded."
  else:
    echo "E5 sample verification failed."
    quit 1
