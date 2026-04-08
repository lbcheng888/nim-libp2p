# LSMR Test Entry Points

LSMR tests are split into deterministic groups so that stable regressions do not
inherit teardown or ordering noise from the slow network cases.

## Stable groups

Run the pure model and data-structure set:

```sh
tests/run_lsmr_tests.sh fast
```

Run the stable service integration set:

```sh
tests/run_lsmr_tests.sh service
```

These are also exposed through Nimble:

```sh
nimble testlsmr
nimble testlsmrservice
```

## Slow network group

Run the split slow network cases:

```sh
tests/run_lsmr_tests.sh slow
```

This group contains:

- `testlsmrservice_network_slow_sync_imports.nim`
- `testlsmrservice_network_slow_near_field.nim`
- `testlsmrservice_network_slow_witness_refresh.nim`
- `testlsmrservice_network_slow_sync_foreign_rooted.nim`
- `testlsmrservice_network_slow_publish_foreign_rooted.nim`

## Explicit single-file runs

Order-sensitive network tests should be run explicitly instead of being folded
back into the stable groups:

```sh
tests/run_lsmr_tests.sh testlsmrservice_network_sync_migration.nim
tests/run_lsmr_tests.sh testlsmrservice_network_sync_self_chain.nim
```

The same entrypoint also supports compile-only mode:

```sh
tests/run_lsmr_tests.sh fast --compile-only
tests/run_lsmr_tests.sh testlsmrservice_network_sync_migration.nim --compile-only
```
