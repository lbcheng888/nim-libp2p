import ../config.nims
import strutils, os

switch("threads", "on")
switch("define", "withoutPCRE")
switch("define", "unittestPrintTime")
when defined(libp2p_test_metrics):
  switch("define", "metrics")
  switch("define", "libp2p_agents_metrics")
  switch("define", "libp2p_protobuf_metrics")
  switch("define", "libp2p_network_protocols_metrics")
  switch("define", "libp2p_mplex_metrics")

# Only add chronicles param if the
# user didn't specify any
var hasChroniclesParam = false
for param in 0 ..< system.paramCount():
  let value = system.paramStr(param)
  if "chronicles" in value and "chronicles_enabled" notin value:
    hasChroniclesParam = true

if hasChroniclesParam:
  echo "Since you specified chronicles params, TRACE won't be tested!"
else:
  let modulePath = currentSourcePath.parentDir / "stublogger"
  if defined(android) or defined(ohos):
    discard
  else:
    switch("import", modulePath)
    switch("undef", "chronicles_streams")
    switch("define", "chronicles_sinks=textlines[stdout],json[dynamic]")
    switch("define", "chronicles_log_level=TRACE")
    switch("define", "chronicles_runtime_filtering=TRUE")
