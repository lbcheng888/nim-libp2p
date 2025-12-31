# to allow locking
switch("path", thisDir())
if dirExists("nimbledeps/pkgs"):
  switch("NimblePath", "nimbledeps/pkgs")
if dirExists("nimbledeps/pkgs2"):
  switch("NimblePath", "nimbledeps/pkgs2")

when defined(android) or defined(ohos):
  switch("path", "compat/chronicles_stub")
else:
  switch("path", "vendor/chronicles_pkg")
switch("path", "vendor/NimYAML")
switch("path", "vendor/jsonschema/src")
switch("path", "nim-pebble")
switch("passC", "-I" & thisDir())
when defined(android) or defined(ohos):
  switch("define", "chronicles_enabled=false")
else:
  switch("define", "chronicles_enabled=true")
when not defined(chronicles_streams):
  switch("define", "chronicles_streams=defaultChroniclesStream[textlines]")

const openSslOutDir = "/Users/lbcheng/openssl/out"
when (not defined(ohos)) and (not defined(android)):
  if dirExists(openSslOutDir):
    switch("passC", "-I" & openSslOutDir & "/include")
    switch("passL", "-L" & openSslOutDir & "/lib")
    switch("passL", "-Wl,-rpath," & openSslOutDir & "/lib")
    switch("passL", "-lssl")
    switch("passL", "-lcrypto")
    when defined(macosx):
      switch("passL", "-Wl,-force_load," & openSslOutDir & "/lib/libssl.a")
      switch("passL", "-Wl,-force_load," & openSslOutDir & "/lib/libcrypto.a")

switch("warningAsError", "UnusedImport:on")
switch("warningAsError", "UseBase:on")
switch("warning", "CaseTransition:off")
switch("warning", "ObservableStores:off")
switch("warning", "LockLevel:off")

--styleCheck:
  usages
--styleCheck:
  error
--mm:
  refc
--warnings:off
--hints:off
  # reconsider when there's a version-2-2 branch worth testing with as we might switch to orc

# Avoid some rare stack corruption while using exceptions with a SEH-enabled
# toolchain: https://github.com/status-im/nimbus-eth2/issues/3121
if defined(windows) and not defined(vcc):
  --define:
    nimRawSetjmp

# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
switch("path", "examples")
switch("path", "examples/bridge")
switch("path", "examples/bridge/bridge")
switch("path", "examples")
switch("path", "examples/bridge")

# ensure nimble dependencies (chronos/chronicles/etc.) are on the search path for standalone tools
import std/os
template addPkgPathIfExists(dirPath: string) =
  if dirExists(dirPath): switch("path", dirPath)
addPkgPathIfExists("nimbledeps/pkgs2/chronos-4.0.4-455802a90204d8ad6b31d53f2efff8ebfe4c834a")
addPkgPathIfExists("nimbledeps/pkgs2/chronos-4.0.4-b10600bf952969f310a69b7d873785f2194e6ff0")
addPkgPathIfExists("nimbledeps/pkgs2/chronicles-0.11.0-e5be8a1a1d79df93be25d7f636d867c70e4ab352")
