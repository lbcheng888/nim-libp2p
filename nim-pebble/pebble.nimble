# Nimble package definition for Pebble Nim rewrite infrastructure
# Provides base modules, testing hooks, and tooling integration.

version       = "0.1.0"
author        = "Pebble Contributors"
description   = "Infrastructure scaffolding for the Pebble Nim rewrite."
license       = "Apache-2.0"

srcDir        = "src"
bin           = @["pebble_cli"]

requires "nim >= 2.3.0"

task build_ci, "Build library and auxiliary binaries for CI":
  exec("nim c --threads:on --path:src --hints:off --warnings:off src/pebble/interop/ab_driver")
  exec("nim c --run --threads:on --path:src --hints:off --warnings:off tests/test_main")

task fuzz_entry, "Run fuzz harness entry point":
  exec("nim c -r --threads:on --path:src --hints:off --warnings:off tests/fuzz_entry")

task docs_formats, "Generate interop format documentation":
  exec("nim r --threads:on --path:src --hints:off --warnings:off src/pebble/interop/format_report")
